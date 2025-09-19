package org.multipaz.testapp

import org.multipaz.compose.mdoc.MdocNfcV2Service
import org.multipaz.mdoc.transport.MdocTransportOptions
import org.multipaz.util.Platform

class TestAppMdocNfcV2Service: MdocNfcV2Service() {
    private lateinit var settingsModel: TestAppSettingsModel

    override suspend fun getSettings(): Settings {
        settingsModel = TestAppSettingsModel.create(
            storage = Platform.storage,
            readOnly = true
        )
        platformCryptoInit(settingsModel)

        return Settings(
            sessionEncryptionCurve = settingsModel.presentmentSessionEncryptionCurve.value,
            allowMultipleRequests = settingsModel.presentmentAllowMultipleRequests.value,
            useNegotiatedHandover = settingsModel.presentmentUseNegotiatedHandover.value,
            negotiatedHandoverPreferredOrder = settingsModel.presentmentNegotiatedHandoverPreferredOrder.value,
            transportOptions = MdocTransportOptions(bleUseL2CAP = settingsModel.presentmentBleL2CapEnabled.value),
            promptModel = platformPromptModel,
            presentmentActivityClass = TestAppMdocNfcV2PresentmentActivity::class.java
        )
    }
}
