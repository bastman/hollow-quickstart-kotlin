package how.hollow.consumer

import com.netflix.hollow.api.client.HollowAnnouncementWatcher
import com.netflix.hollow.api.client.HollowBlobRetriever
import com.netflix.hollow.api.client.HollowClient
import com.netflix.hollow.core.read.engine.HollowReadStateEngine
import com.netflix.hollow.explorer.ui.jetty.HollowExplorerUIServer
import com.netflix.hollow.history.ui.jetty.HollowHistoryUIServer
import how.hollow.consumer.api.generated.MovieAPI
import how.hollow.consumer.history.ConsumerHistoryListener
import how.hollow.consumer.infrastructure.FilesystemAnnouncementWatcher
import how.hollow.consumer.infrastructure.FilesystemBlobRetriever
import how.hollow.producer.Producer

import java.io.File

fun main(args: Array<String>) {
    val publishDir = File(Producer.SCRATCH_DIR, "publish-dir")
    println("I AM THE CONSUMER.  I WILL READ FROM " + publishDir.absolutePath)

    val consumer = ConsumerKotlin(
            blobRetriever=FilesystemBlobRetriever(publishDir),
            announcementWatcher = FilesystemAnnouncementWatcher(publishDir)
    )

    consumer.run()
}

class ConsumerKotlin(blobRetriever: HollowBlobRetriever, announcementWatcher: HollowAnnouncementWatcher) {

    private val historyListener: ConsumerHistoryListener= ConsumerHistoryListener()

    private val client: HollowClient=HollowClient.Builder()
            .withBlobRetriever(blobRetriever)
            .withAnnouncementWatcher(announcementWatcher)
            .withUpdateListener(historyListener)
            .withGeneratedAPIClass(MovieAPI::class.java)
            .build()

    private fun api(): MovieAPI = client.api as MovieAPI
    private fun stateEngine(): HollowReadStateEngine = client.stateEngine

    private fun initialize() = client.triggerRefresh()

    fun run() {
        initialize()

        println("THERE ARE " + api().allMovieHollow.size + " MOVIES IN THE DATASET AT STARTUP")

        api().allActorHollow.forEach { it -> println("actor: " + it._getActorName()._getValue()) }

        startHistoryUiServer()
        startExplorerUiServer()
    }

    private fun startHistoryUiServer() {
        /// start a Hollow History UI server (point your browser to http://localhost:7777)
        val historyUIServer = HollowHistoryUIServer(historyListener.history, 7777)
        historyUIServer.start()
    }

    private fun startExplorerUiServer() {
        /// start a Hollow Explorer UI server (point your browser to http://localhost:7778)
        val explorerUIServer = HollowExplorerUIServer(client, 7778)
        explorerUIServer.start()

        explorerUIServer.join()
    }
}