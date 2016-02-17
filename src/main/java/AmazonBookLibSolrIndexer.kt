package biz.ziak.AmazonLibraryThingSolrIndexer

import java.util.concurrent.Executors
import java.util.logging.Logger
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.common.SolrInputDocument
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.io.File
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.logging.Level
import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory

class AmazonBookLibSolrIndexer(val path: String, val solrUri: String) {
    val LOGGER = Logger.getLogger(AmazonBookLibSolrIndexer::class.qualifiedName)

    var server: HttpSolrClient? =null
    var threadPool = Executors.newFixedThreadPool(10)

    fun indexAllFilesFromPath(){
        createServerAndWipeIndex()
        val file = File(path)
        readFiles(file)
    }

    private fun readFiles(file:File) {
        var futures = ArrayList<Future<Void>>()
        if(file.isDirectory){
            var dirIt= file?.listFiles()?.iterator()
            try{
                while(dirIt?.hasNext()!!){
                    readFiles(dirIt!!.next())
                }
            }catch(e :Exception){
                LOGGER.log(Level.WARNING,"Something went wrong",e)
            }
        }else if(file.isFile){
            futures.add(threadPool?.submit(Callable<Void> {  parseFile(file) })!!)
        }
        futures.forEach {it.get()}

    }

    /**
     * that's an ugly parsing of the xml
     */
    private fun parseFile(file: File): Void? {
        var id : String
        var isbn :String? = null
        var title :String? = null
        var ean  :String? = null
        var binding :String
        var label :String
        var listprice :String
        var manufacturer :String
        var publisher :String
        var studio :String
        var readinglevel :String
        var releasedate :String
        var publicationdate :String
        var edition :String
        var dewey :String
        var numberofpages :String
        var dimheight :String
        var dimwidth :String
        var dimlength :String
        var dimweight :String
        var series :String
        var tags :String
        var editorialreviews :String
        var userreviews :String
        var userreviewsauthorID :String
        var creators :String
        var subjects :String
        var similarproducts :String
        var browseNodes :String
        var browseNodesID :String


        val dbFactory = DocumentBuilderFactory.newInstance();
        val dBuilder = dbFactory.newDocumentBuilder();
        val doc = dBuilder.parse(file);
        val solrDoc = SolrInputDocument()
        doc.getDocumentElement().normalize();

        title  = extractElement(doc, solrDoc, "title")
        isbn = extractElement(doc, solrDoc,"isbn")
        ean = extractElement(doc, solrDoc,"ean")
        solrDoc.addField("id", isbn+""+title+""+ean)
        extractElement(doc, solrDoc,"binding")
        extractElement(doc, solrDoc,"label")
        extractElement(doc, solrDoc,"listprice")
        extractElement(doc, solrDoc,"manufacturer")
        extractElement(doc, solrDoc,"publisher")
        extractElement(doc, solrDoc,"readinglevel")
        extractElement(doc, solrDoc,"releasedate")
        extractElement(doc, solrDoc,"publicationdate")
        extractElement(doc, solrDoc,"studio")
        extractElement(doc, solrDoc,"edition")
        extractElement(doc, solrDoc,"dewey")
        extractElement(doc, solrDoc,"numberofpages")
        extractElement(doc, solrDoc,"subject")


        server?.add(solrDoc)





        return null
    }

    private fun extractElement(doc: Document, solrDoc: SolrInputDocument, element: String?): String? {
        var text : String? = null
        var nlist = doc.getElementsByTagName(element)
        var y = 0
        while (y < nlist.length) {
            text = nlist.item(y).textContent
            solrDoc.addField(element, nlist.item(y).textContent)
            y++
        }
        return text
    }

    /**
     * creates the server and wipes the index as well
     */
    private fun createServerAndWipeIndex() {
        server = HttpSolrClient(solrUri)
        server!!.deleteByQuery("*:*")
        server!!.commit()
    }


}




fun main(args: Array<String>){
    if(args.size==2) {
        val amazonLibIndexer = AmazonBookLibSolrIndexer(args[0],args[1])
        amazonLibIndexer.indexAllFilesFromPath()
    }else{
        System.out.println("Not enough parameter: Usage programm <Path> <SolrURL>")
    }

}

