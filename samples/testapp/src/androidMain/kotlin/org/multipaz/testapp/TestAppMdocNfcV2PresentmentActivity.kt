package org.multipaz.testapp

import androidx.compose.runtime.Composable
import coil3.ImageLoader
import coil3.network.ktor2.KtorNetworkFetcherFactory
import io.ktor.client.HttpClient
import org.multipaz.compose.mdoc.MdocNfcV2Service
import org.multipaz.compose.mdoc.MdocNfcPresentmentActivity
import org.multipaz.testapp.ui.AppTheme

class TestAppMdocNfcV2PresentmentActivity : MdocNfcPresentmentActivity() {
    override suspend fun getSettings(): Settings {
        val app = App.getInstance()
        app.init()
        val imageLoader = ImageLoader.Builder(applicationContext).components {
            add(KtorNetworkFetcherFactory(HttpClient(platformHttpClientEngineFactory().create())))
        }.build()
        return Settings(
            appName = platformAppName,
            appIcon = platformAppIcon,
            promptModel = app.promptModel,
            applicationTheme = @Composable { AppTheme(it) },
            documentTypeRepository = app.documentTypeRepository,
            presentmentModel = MdocNfcV2Service.presentmentModel,
            presentmentSource = app.getPresentmentSource(),
            imageLoader = imageLoader
        )
    }
}