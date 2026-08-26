package com.andrerinas.openheadunit.utils

import android.content.Context
import android.net.MacAddress
import android.net.wifi.SoftApConfiguration
import android.net.wifi.WifiManager
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import com.andrerinas.openheadunit.aap.HotspotBandPreference
import com.andrerinas.openheadunit.aap.SoftApBandPolicy
import com.andrerinas.openheadunit.aap.SoftApBssidPolicy

/**
 * Starts and stops this device's own private WiFi access point via
 * `WifiManager.startLocalOnlyHotspot(SoftApConfiguration, Executor, LocalOnlyHotspotCallback)`,
 * reflectively — exactly the mechanism `hotspot-helper` used to own, and the only one confirmed
 * to work on this hardware without root or WRITE_SETTINGS/TETHER_PRIVILEGED. The previous
 * implementation went through system tethering (TetheringManager/ConnectivityManager/
 * setWifiApEnabled), which is privileged and never actually worked here — including on
 * [restart], which is why a dropped session used to leave the phone stuck on a dead network for
 * 15-27 seconds instead of reconnecting immediately.
 */
object HotspotManager {
    /** How long to wait for the async start callback before giving up. */
    private const val START_TIMEOUT_MS = 6_000L

    /**
     * How long to let a just-closed access point actually finish tearing down before starting a
     * new one. `close()` only posts the teardown; measured on this hardware, `SoftApManager`
     * confirms "Soft AP is stopped" 90-180ms later, and asking for a new access point before that
     * — even a few ms before — collides with the old one mid-teardown (`onFailed`, reason 2), so
     * this needs headroom rather than a tight bound. Not driven off a confirmation signal: our own
     * callback's `onStopped()` is measured to never fire for our own `close()` on this hardware
     * (only for a system-initiated stop), and polling `getWifiApState()` proved just as
     * unreliable — it returned "confirmed" within 1ms of `close()`, before the framework had done
     * anything, on a device where the same call works fine everywhere else in this codebase. A
     * fixed wait with real headroom over the measured worst case is what's actually reliable here.
     */
    private const val RESTART_SETTLE_MS = 300L

    /** Hidden `SoftApConfiguration` constant, not present in the public stub jar. */
    private const val RANDOMIZATION_NONE = 0

    /** An access point we started, plus the latch that fires when the framework actually tears
     *  it down — whether from our own [close] or the system stopping it on its own. */
    private class ActiveHotspot(
        val reservation: WifiManager.LocalOnlyHotspotReservation,
        val stoppedLatch: CountDownLatch = CountDownLatch(1)
    )

    @Volatile private var active: ActiveHotspot? = null

    /**
     * Whether a start is already running. Bringing an access point up takes up to
     * [START_TIMEOUT_MS], and more than one caller asks for it: the credentials provider
     * auto-enables, and AapService asks again on its own paths.
     */
    @Volatile private var startInFlight = false

    /**
     * Identifies the most recent start attempt. `startInFlight` keeps two [setHotspotEnabled]
     * (true) calls from overlapping, but a [WifiManager.LocalOnlyHotspotCallback] belonging to
     * an *older* attempt can still fire — usually `onStopped()` — long after that attempt has
     * finished and a newer one has started, since `restart()` moves on to a new start well
     * before the old reservation's real teardown callback is guaranteed to have arrived (see
     * [RESTART_SETTLE_MS]). Each callback closure captures the generation current when it was
     * registered and checks it before touching [active], so a late callback from a superseded
     * attempt cannot null out — or otherwise clobber — a newer, still-live reservation. Mirrors
     * `HotspotServiceController.acceptsCallback()` in the sibling `hotspot-helper` app, which
     * solves the identical problem the same way.
     *
     * Also bumped by [setHotspotEnabled]'s `enabled = false` path, even when it finds [active]
     * already null. Without that, a disable racing a start still in flight — `active` not set
     * yet, so the disable sees "nothing to close" and returns immediately — would let that
     * start's `onStarted()` land afterwards and quietly turn the hotspot back on, with nothing
     * left holding a reference that would ever close it again. Bumping the generation here means
     * that `onStarted()`, whenever it does arrive, finds itself superseded and closes its own
     * reservation instead of publishing it — the same path already used for a start superseded
     * by a newer start, just triggered by a disable instead.
     */
    private val startGeneration = AtomicLong(0)

    private val lock = Any()

    /**
     * Where the framework dispatches [WifiManager.LocalOnlyHotspotCallback]. Deliberately not
     * `context.mainExecutor`: [setHotspotEnabled] with `enabled = false` blocks its caller on
     * [ActiveHotspot.stoppedLatch], and `AapService.onDestroy()` calls it straight from the main
     * thread — dispatching the callback onto that same main thread would have it wait on itself.
     */
    private val callbackExecutor = Executors.newSingleThreadExecutor { r -> Thread(r, "HotspotManager-callback") }

    /**
     * Takes the access point down and brings it straight back up, so any joined client is put
     * off it.
     *
     * [setHotspotEnabled] with `enabled = false` here closes an owned
     * [WifiManager.LocalOnlyHotspotReservation] and — this is the part that matters for a
     * restart — gives it [RESTART_SETTLE_MS] to actually finish tearing down before returning.
     * `close()` only posts the teardown; asking for a new access point immediately raced it here
     * (`onFailed`, reason 2, a few ms after `close()`) and needed a second, unrelated auto-enable
     * elsewhere in the app to recover the hotspot at all, ten seconds later. See
     * [RESTART_SETTLE_MS] for why this is a fixed wait rather than a polled confirmation.
     */
    fun restart(context: Context): Boolean {
        AppLog.i("HotspotManager: Restarting the hotspot so any joined client is put off it.")
        setHotspotEnabled(context, false)
        return setHotspotEnabled(context, true)
    }

    fun setHotspotEnabled(context: Context, enabled: Boolean): Boolean {
        if (!enabled) {
            val current = synchronized(lock) {
                // Supersede any start still in flight before reading `active`, not after — see
                // [startGeneration]. Unconditional: whether or not one turns out to be running,
                // bumping the generation here is what keeps a start that has not reached
                // onStarted() yet from adopting `active` after this call has already returned.
                startGeneration.incrementAndGet()
                active
            }
            if (current == null) return true
            current.reservation.close()
            awaitTeardown(current.stoppedLatch)
            synchronized(lock) { if (active === current) active = null }
            return true
        }

        synchronized(lock) {
            if (startInFlight) {
                AppLog.i("HotspotManager: A hotspot start is already running; letting it finish rather than starting a second one.")
                return active != null
            }
            startInFlight = true
        }

        try {
            val ssid = Settings(context).hotspotSsid.trim()
            val password = Settings(context).hotspotPassword.trim()
            if (ssid.isEmpty() || password.isEmpty()) {
                AppLog.e("HotspotManager: Cannot start the hotspot without a configured name and password (Settings > Hotspot). Refusing to fall back to the system tethering config, since that would not match what this app tells the phone to connect to.")
                return false
            }

            val preference = bandPreference(context)
            val band = SoftApBandPolicy.attemptOrder(preference).first()
            AppLog.i("HotspotManager: Band preference is ${SoftApBandPolicy.describePreference(preference)}; requesting ${SoftApBandPolicy.describe(band)}.")

            val staticBssid = Settings(context).staticBSSID
            val bssid = if (SoftApBssidPolicy.isUsable(staticBssid)) staticBssid else null

            val config = try {
                buildSoftApConfiguration(ssid, password, SoftApBandPolicy.softApConfigurationBand(band), bssid)
            } catch (t: Throwable) {
                AppLog.e("HotspotManager: Could not build a SoftApConfiguration via reflection.", t)
                return false
            }

            val startLatch = CountDownLatch(1)
            val started = AtomicBoolean(false)
            // Belongs to the ActiveHotspot this start produces, if it produces one — created
            // upfront so onFailed can still release anyone waiting on a *previous* ActiveHotspot's
            // stoppedLatch to have no effect here (they're different objects; see onStarted).
            val stoppedLatch = CountDownLatch(1)
            // Captured now, while startInFlight still guarantees no other attempt can be running:
            // this is the generation this callback's onStarted/onFailed/onStopped must still match
            // by the time they fire, or a late one is stale and must not touch shared state.
            val myGeneration = startGeneration.incrementAndGet()

            val callback = object : WifiManager.LocalOnlyHotspotCallback() {
                override fun onStarted(res: WifiManager.LocalOnlyHotspotReservation) {
                    started.set(true)
                    startLatch.countDown()
                    if (myGeneration != startGeneration.get()) {
                        AppLog.w("HotspotManager: This start was superseded before it finished (a newer start or an explicit disable arrived first); closing the now-redundant reservation instead of taking it over.")
                        res.close()
                        return
                    }
                    synchronized(lock) { active = ActiveHotspot(res, stoppedLatch) }
                    AppLog.i("HotspotManager: Hotspot started. ssid=${describeReservation(res)}")
                }

                override fun onFailed(reason: Int) {
                    startLatch.countDown()
                    if (myGeneration != startGeneration.get()) return
                    synchronized(lock) { active = null }
                    AppLog.e("HotspotManager: Hotspot start failed, reason=$reason")
                }

                override fun onStopped() {
                    stoppedLatch.countDown()
                    // Measured on this hardware: onStopped() for our own close() routinely arrives
                    // after RESTART_SETTLE_MS has already elapsed and restart() has moved on to a
                    // new attempt. Without this check, that late call would null out `active` while
                    // it correctly points at the newer, still-live reservation — orphaning it, since
                    // setHotspotEnabled(false) is then a no-op forever after (active == null) and the
                    // framework never gets a close() for it.
                    if (myGeneration != startGeneration.get()) {
                        AppLog.w("HotspotManager: A stale onStopped() arrived for a superseded hotspot attempt; ignoring it instead of clearing the current one.")
                        return
                    }
                    synchronized(lock) { active = null }
                    AppLog.w("HotspotManager: Hotspot was stopped (by the system or another caller).")
                }
            }

            try {
                val wifiManager = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
                val startMethod = WifiManager::class.java.getMethod(
                    "startLocalOnlyHotspot",
                    SoftApConfiguration::class.java,
                    Executor::class.java,
                    WifiManager.LocalOnlyHotspotCallback::class.java
                )
                startMethod.invoke(wifiManager, config, callbackExecutor, callback)
            } catch (t: Throwable) {
                AppLog.e("HotspotManager: startLocalOnlyHotspot() is unavailable via reflection.", t)
                return false
            }

            if (!startLatch.await(START_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                AppLog.w("HotspotManager: Timed out after ${START_TIMEOUT_MS / 1000}s waiting for the hotspot to start.")
                return false
            }
            return started.get()
        } finally {
            startInFlight = false
        }
    }

    /**
     * Gives a just-closed access point [RESTART_SETTLE_MS] to actually finish tearing down —
     * unless [stoppedLatch] fires first (belt-and-suspenders for a device where `onStopped()`
     * does fire for our own `close()`, letting the wait end early there instead of always paying
     * the full settle time).
     */
    private fun awaitTeardown(stoppedLatch: CountDownLatch) {
        try {
            stoppedLatch.await(RESTART_SETTLE_MS, TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    /** Builds a `SoftApConfiguration` via reflection, identically to `hotspot-helper`. */
    private fun buildSoftApConfiguration(ssid: String, password: String, band: Int, bssid: String?): SoftApConfiguration {
        val builderClass = Class.forName("android.net.wifi.SoftApConfiguration\$Builder")
        val builder = builderClass.getDeclaredConstructor().newInstance()
        builderClass.getMethod("setSsid", String::class.java).invoke(builder, ssid)
        builderClass.getMethod("setPassphrase", String::class.java, Int::class.javaPrimitiveType)
            .invoke(builder, password, SoftApConfiguration.SECURITY_TYPE_WPA2_PSK)
        builderClass.getMethod("setBand", Int::class.javaPrimitiveType).invoke(builder, band)
        builderClass.getMethod("setAutoShutdownEnabled", Boolean::class.javaPrimitiveType).invoke(builder, false)
        builderClass.getMethod("setMacRandomizationSetting", Int::class.javaPrimitiveType).invoke(builder, RANDOMIZATION_NONE)
        if (bssid != null) {
            builderClass.getMethod("setBssid", MacAddress::class.java).invoke(builder, MacAddress.fromString(bssid))
        }
        return builderClass.getMethod("build").invoke(builder) as SoftApConfiguration
    }

    /**
     * Reads SSID/BSSID/band off the granted reservation for logging, entirely via reflection —
     * `SoftApConfiguration`'s getters are not reliably part of the public stub across compileSdk
     * levels (see [HotspotConfigReader], which does the same for the same reason).
     */
    private fun describeReservation(res: WifiManager.LocalOnlyHotspotReservation): String = try {
        val configClass = Class.forName("android.net.wifi.SoftApConfiguration")
        val config = res.javaClass.getMethod("getSoftApConfiguration").invoke(res)
        val ssid = configClass.getMethod("getSsid").invoke(config)
        val bssid = try { configClass.getMethod("getBssid").invoke(config) } catch (e: Exception) { "?" }
        val band = try { configClass.getMethod("getBand").invoke(config) } catch (e: Exception) { "?" }
        "ssid=$ssid, bssid=$bssid, band=$band"
    } catch (e: Exception) {
        "<unavailable: ${e.message}>"
    }

    /**
     * The user's band choice, or [HotspotBandPreference.AUTO] if it cannot be read.
     *
     * The catch is not decoration: `Settings` lives in credential-encrypted storage and throws
     * outright before the user has unlocked the device, and this runs from the boot and auto-start
     * paths. Falling back to the automatic sweep there is the same behaviour the app had before
     * this setting existed.
     */
    private fun bandPreference(context: Context): HotspotBandPreference = try {
        HotspotBandPreference.fromSetting(Settings(context).hotspotBand)
    } catch (e: Exception) {
        AppLog.w("HotspotManager: Could not read the band preference (${e.message}); asking for 5 GHz and falling back.")
        HotspotBandPreference.AUTO
    }
}
