package AmazonBookLibSolrIndexerr

import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.common.SolrInputDocument
import org.w3c.dom.Document
import java.io.File
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import javax.xml.parsers.DocumentBuilderFactory

class AmazonBookLibSolrIndexer(val path: String, val solrUri: String) {
    val LOGGER = Logger.getLogger(AmazonBookLibSolrIndexer::class.qualifiedName)

    var server: HttpSolrClient? = null
    var threadPool = Executors.newFixedThreadPool(20)
    var futures = ArrayList<Future<Void>>()


    fun indexAllFilesFromPath() {
        createServerAndWipeIndex()
        val file = File(path)
        readFiles(file)
        futures.forEachIndexed { i, future ->
            future.get(1000, TimeUnit.SECONDS)

            if (i % 1000 == 0) {

                server?.commit();
            }
            server?.commit();
        }
    }

    /**
     * reads the amazon-lt-xml-all-no-images-1.1 files
     */
    private fun readFiles(file: File) {

        if (file.isDirectory) {
            var dirIt = file?.listFiles()?.iterator()
            try {
                while (dirIt?.hasNext()!!) {
                    readFiles(dirIt!!.next())
                }
            } catch(e: Exception) {
                LOGGER.log(Level.WARNING, "Something went wrong", e)
            }
        } else if (file.isFile) {
            futures.add(threadPool?.submit(Callable<Void> { parseFile(file) })!!);

        }
    }


    /**
     * that's an ugly parsing of the xml
     */
    private fun parseFile(file: File): Void? {

        var isbn: String? = null
        var title: String? = null
        var ean: String? = null


        val dbFactory = DocumentBuilderFactory.newInstance();
        val dBuilder = dbFactory.newDocumentBuilder();
        val doc = dBuilder.parse(file);
        val solrDoc = SolrInputDocument()
        doc.getDocumentElement().normalize();

        title = extractElement(doc, solrDoc, "title", file)
        isbn = extractElement(doc, solrDoc, "isbn", file)
        ean = extractElement(doc, solrDoc, "ean", file)
        solrDoc.addField("id", isbn + "" + title + "" + ean)
        extractElement(doc, solrDoc, "binding", file)
        extractElement(doc, solrDoc, "label", file)
        extractElement(doc, solrDoc, "listprice", file)
        extractElement(doc, solrDoc, "manufacturer", file)
        extractElement(doc, solrDoc, "publisher", file)
        extractElement(doc, solrDoc, "readinglevel", file)
        extractElement(doc, solrDoc, "releasedate", file)
        extractElement(doc, solrDoc, "publicationdate", file)
        extractElement(doc, solrDoc, "studio", file)
        extractElement(doc, solrDoc, "edition", file)
        extractElement(doc, solrDoc, "dewey", file)
        extractElement(doc, solrDoc, "numberofpages", file)
        extractElement(doc, solrDoc, "subject", file)
        extractElement(doc, solrDoc, "tag", file, "count")
        extractElement(doc, solrDoc, "browseNode", file)
        extractElement(doc, solrDoc, "similarproduct", file)
        extractElement(doc, solrDoc, "seriesitem", file)
        extractMultiElement(doc, solrDoc, "creator", file)
        extractMultiElement(doc, solrDoc, "editorialreview", file)
        server?.add(solrDoc)
        return null
    }

    /**
     * Extracts elements that contains multiple subnodes
     */
    private fun extractMultiElement(doc: Document?, solrDoc: SolrInputDocument, element: String?, file: File): String? {
        var text: String? = null
        try {
            var nlist = doc?.getElementsByTagName(element)
            var y = 0
            while (y < nlist!!.length) {
                var childList = nlist.item(y).childNodes
                var childTextualContent = ""
                var x = 0
                while (x < childList.length) {
                    childTextualContent += childList.item(x).textContent + ": "
                    x++
                }
                solrDoc.addField(element, childTextualContent)
                y++
            }
        } catch(e: Exception) {
            throw Exception(file.canonicalPath + " " + e.message)
        }
        return text

    }

    /**
     * extracts the element of the xml doc
     */
    private fun extractElement(doc: Document, solrDoc: SolrInputDocument, element: String?, file: File, attribute: String? = null): String? {
        var text: String? = null
        try {
            var nlist = doc.getElementsByTagName(element)
            var attribute: String? = null

            var y = 0
            while (y < nlist.length) {
                text = nlist.item(y).textContent
                if (nlist.item(y).hasAttributes() && attribute != null) {
                    attribute = nlist.item(y)!!.attributes!!.getNamedItem(attribute).nodeValue;
                }
                if (attribute != null && text != null && text.trim().length > 0) {
                    solrDoc.addField(element, text + " " + attribute)
                } else if (text != null && text.trim().length > 0) {

                    solrDoc.addField(element, text)
                }

                if (y != (nlist.length - 1)) {
                    text = null
                    attribute = null
                }

                y++
            }
        } catch(e: Exception) {
            throw Exception(file.canonicalPath + " " + e.message)
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


fun main(args: Array<String>) {
    if (args.size == 2) {
        val amazonLibIndexer = AmazonBookLibSolrIndexer(args[0], args[1])
        amazonLibIndexer.indexAllFilesFromPath()
    } else {
        System.out.println("Not enough parameter: Usage programm <PathToAmazonLibFiles> <SolrURL>")
        System.out.println("e.g. /opt/Datasets/amazon-lt-xml-all-no-images-1.1/xml http://localhost:8983/solr/amazonLib")
    }

}

