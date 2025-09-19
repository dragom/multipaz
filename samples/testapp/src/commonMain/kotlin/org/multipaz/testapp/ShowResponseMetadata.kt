package org.multipaz.testapp

import org.multipaz.cbor.annotation.CborSerializable

@CborSerializable
data class ShowResponseMetadata(
    val engagementType: String,
    val transferProtocol: String,
    val requestSize: Long,
    val responseSize: Long,
    val requestSentViaNfcV2: Boolean?,
    val responseReceivedViaNfcV2: Boolean?,
    val durationMsecNfcTapToEngagement: Long?,
    val durationMsecEngagementReceivedToRequestSent: Long?,
    val durationMsecRequestSentToResponseReceived: Long
) {
    companion object
}