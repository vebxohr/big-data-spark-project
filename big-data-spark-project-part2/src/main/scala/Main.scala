import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.Base64
import java.time
import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer

// Stopwords list from -> https://gist.github.com/sebleier/554280





object Main{
  val englishStopWords =
    List("a","about","above","after","again","against","ain","all","am","an","and",
      "any","are","aren","aren't","as","at","be","because","been","before","being",
      "below","between","both","but","by","can","couldn","couldn't","d","did","didn",
      "didn't","do","does","doesn","doesn't","doing","don","don't","down","during",
      "each","few","for","from","further","had","hadn","hadn't","has","hasn","hasn't",
      "have","haven","haven't","having","he","her","here","hers","herself","him",
      "himself","his","how","i","if","in","into","is","isn","isn't","it","it's","its",
      "itself","just","ll","m","ma","me","mightn","mightn't","more","most","mustn",
      "mustn't","my","myself","needn","needn't","no","nor","not","now","o","of","off",
      "on","once","only","or","other","our","ours","ourselves","out","over","own","re"
      ,"s","same","shan","shan't","she","she's","should","should've","shouldn",
      "shouldn't","so","some","such","t","than","that","that'll","the","their",
      "theirs","them","themselves","then","there","these","they","this","those",
      "through","to","too","under","until","up","ve","very","was","wasn","wasn't","we"
      ,"were","weren","weren't","what","when","where","which","while","who","whom",
      "why","will","with","won","won't","wouldn","wouldn't","y","you","you'd","you'll"
      ,"you're","you've","your","yours","yourself","yourselves","could","he'd","he'll"
      ,"he's","here's","how's","i'd","i'll","i'm","i've","let's","ought","she'd",
      "she'll","that's","there's","they'd","they'll","they're","they've","we'd",
      "we'll","we're","we've","what's","when's","where's","who's","why's","would",
      "able","abst","accordance","according","accordingly","across","act","actually",
      "added","adj","affected","affecting","affects","afterwards","ah","almost",
      "alone","along","already","also","although","always","among","amongst",
      "announce","another","anybody","anyhow","anymore","anyone","anything","anyway",
      "anyways","anywhere","apparently","approximately","arent","arise","around",
      "aside","ask","asking","auth","available","away","awfully","b","back","became",
      "become","becomes","becoming","beforehand","begin","beginning","beginnings",
      "begins","behind","believe","beside","besides","beyond","biol","brief","briefly"
      ,"c","ca","came","cannot","can't","cause","causes","certain","certainly","co",
      "com","come","comes","contain","containing","contains","couldnt","date",
      "different","done","downwards","due","e","ed","edu","effect","eg","eight",
      "eighty","either","else","elsewhere","end","ending","enough","especially","et",
      "etc","even","ever","every","everybody","everyone","everything","everywhere",
      "ex","except","f","far","ff","fifth","first","five","fix","followed","following"
      ,"follows","former","formerly","forth","found","four","furthermore","g","gave",
      "get","gets","getting","give","given","gives","giving","go","goes","gone","got",
      "gotten","h","happens","hardly","hed","hence","hereafter","hereby","herein",
      "heres","hereupon","hes","hi","hid","hither","home","howbeit","however",
      "hundred","id","ie","im","immediate","immediately","importance","important",
      "inc","indeed","index","information","instead","invention","inward","itd",
      "it'll","j","k","keep","keeps","kept","kg","km","know","known","knows","l",
      "largely","last","lately","later","latter","latterly","least","less","lest",
      "let","lets","like","liked","likely","line","little","'ll","look","looking",
      "looks","ltd","made","mainly","make","makes","many","may","maybe","mean","means"
      ,"meantime","meanwhile","merely","mg","might","million","miss","ml","moreover",
      "mostly","mr","mrs","much","mug","must","n","na","name","namely","nay","nd",
      "near","nearly","necessarily","necessary","need","needs","neither","never",
      "nevertheless","new","next","nine","ninety","nobody","non","none","nonetheless",
      "noone","normally","nos","noted","nothing","nowhere","obtain","obtained",
      "obviously","often","oh","ok","okay","old","omitted","one","ones","onto","ord",
      "others","otherwise","outside","overall","owing","p","page","pages","part",
      "particular","particularly","past","per","perhaps","placed","please","plus",
      "poorly","possible","possibly","potentially","pp","predominantly","present",
      "previously","primarily","probably","promptly","proud","provides","put","q",
      "que","quickly","quite","qv","r","ran","rather","rd","readily","really","recent"
      ,"recently","ref","refs","regarding","regardless","regards","related",
      "relatively","research","respectively","resulted","resulting","results","right",
      "run","said","saw","say","saying","says","sec","section","see","seeing","seem",
      "seemed","seeming","seems","seen","self","selves","sent","seven","several",
      "shall","shed","shes","show","showed","shown","showns","shows","significant",
      "significantly","similar","similarly","since","six","slightly","somebody",
      "somehow","someone","somethan","something","sometime","sometimes","somewhat",
      "somewhere","soon","sorry","specifically","specified","specify","specifying",
      "still","stop","strongly","sub","substantially","successfully","sufficiently",
      "suggest","sup","sure","take","taken","taking","tell","tends","th","thank",
      "thanks","thanx","thats","that've","thence","thereafter","thereby","thered",
      "therefore","therein","there'll","thereof","therere","theres","thereto",
      "thereupon","there've","theyd","theyre","think","thou","though","thoughh",
      "thousand","throug","throughout","thru","thus","til","tip","together","took",
      "toward","towards","tried","tries","truly","try","trying","ts","twice","two","u"
      ,"un","unfortunately","unless","unlike","unlikely","unto","upon","ups","us",
      "use","used","useful","usefully","usefulness","uses","using","usually","v",
      "value","various","'ve","via","viz","vol","vols","vs","w","want","wants","wasnt"
      ,"way","wed","welcome","went","werent","whatever","what'll","whats","whence",
      "whenever","whereafter","whereas","whereby","wherein","wheres","whereupon",
      "wherever","whether","whim","whither","whod","whoever","whole","who'll",
      "whomever","whos","whose","widely","willing","wish","within","without","wont",
      "words","world","wouldnt","www","x","yes","yet","youd","youre","z","zero","a's",
      "ain't","allow","allows","apart","appear","appreciate","appropriate",
      "associated","best","better","c'mon","c's","cant","changes","clearly",
      "concerning","consequently","consider","considering","corresponding","course",
      "currently","definitely","described","despite","entirely","exactly","example",
      "going","greetings","hello","help","hopefully","ignored","inasmuch","indicate",
      "indicated","indicates","inner","insofar","it'd","keep","keeps","novel",
      "presumably","reasonably","second","secondly","sensible","serious","seriously",
      "sure","t's","third","thorough","thoroughly","three","well","wonder")

  def LoadCsvToRDD(filePath: String, sparkContext: SparkContext): RDD[Array[String]] = {
    val RDD = sparkContext.textFile(filePath)
    val removedHeader = RDD.mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
    removedHeader.map(string => string.split('\t'))
  }

  // Function to convert strings to int, and NULL to -1
  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => -1
    }
  }


  // Convert string of date to LocalDateTime
  def StringToDateTime(dateTimeString: String): Option[time.LocalDateTime] = {
    if (dateTimeString.equals("NULL")){
      return None
    }
    val dateTimeArray= dateTimeString.split(" ")
    val dateArray = dateTimeArray(0).split("-")
    val timeArray = dateTimeArray(1).split(":")
    Some(time.LocalDateTime.of(dateArray(0).toInt, dateArray(1).toInt, dateArray(2).toInt, timeArray(0).toInt,
      timeArray(1).toInt, timeArray(2).toInt))
  }

  def main(args: Array[String]): Unit = {
    // Set up spark session and context
    val spark = SparkSession.builder.appName("TDT4305").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    if (args.length != 2){
      println("Please input data path and post id")

      System.exit(0)

    }

    // Input arguments from command line
    val dataPath = args(0)
    val postID = toInt(args(1))


    // Id, PostTypeId, CreationDate, Score, ViewCount, Body, OwnerUserId, LastActivityDate, Title, Tags, AnswerCount,
    // CommentCount, FavoriteCount, CloseDate
    val posts = LoadCsvToRDD(dataPath + "posts.csv.gz", sc).map(row => (toInt(row(0)), toInt(row(1)),
      StringToDateTime(row(2)),toInt(row(3)), toInt(row(4)), new String(Base64.getDecoder.decode(row(5))),
      toInt(row(6)), StringToDateTime(row(7)), row(9), row(10), toInt(row(11)), toInt(row(12)),
      StringToDateTime(row(13))))

    val post = posts.filter(row => row._1 == postID)

    // regex that removes html tags = "&#xa|<[^>]*>|\\p{Punct}|\\t"
    // to lower case, remove punctuation marks (except DOT) and tab character
    val text = post.map(row => row._6.toLowerCase().replaceAll("""[\p{Punct}&&[^.]]""", "").
      replaceAll("\t", ""))


    // Function to remove first and last dots of words
    def RemoveDots(string: String) : String = {
      var newString = string
      if (newString.substring(0, 1) == "."){
        newString = newString.substring(1)
      }
      if (newString.substring(newString.length-1, newString.length) == "."){
        newString = newString.substring(0, newString.length - 1)
      }
      newString
    }

    // Create RDD with one row for each token
    val tokens = text.flatMap(row => row.split(" "))
      .filter(token => token.length >= 3)  // Remove tokens with <3 characters
      .map(token => RemoveDots(token))  // Remove dots and beginning and end
      .filter(token => !englishStopWords.contains(token)) // Remove stopwords

    val tokenList = tokens.collect()
    // val tokenList = Array("data", "science", "discussed", "forum", "synonyms", "fields", "large", "data", "analyzed", "question", "data", "mining", "graduate", "class", "data", "mining", "years", "differences", "data", "science", "data", "mining", "proficient", "data", "mining")

    val nodes = tokenList.distinct
    val nodesID  = nodes zip List.range(0, nodes.length)
    val nodesIDMap = nodesID.toMap

    // Create all pairs of tokens within a sliding window and return distinct ones
    def getEdges(tokens : Array[String], windowSize : Int) : List[(Option[Int], Option[Int])] = {
      var edges = new ListBuffer[(Option[Int], Option[Int])]()
      val numChunks = tokens.length - windowSize + 1
      // val tokens = seq zip Array.fill(windowSize)(1)
      for (i <- 0 until numChunks){
        val window = tokens.slice(i, i + windowSize)

        for (j <- window.indices){
          for (k <- j + 1 until (window.length)) {
            val v1 = nodesIDMap.get(window(j))
            val v2 = nodesIDMap.get(window(k))

            if (v1 != v2){
              // Dont include edges to self
              val t = (v1, v2)
              val t2 = (v2, v1)
              edges += t
              edges += t2
            }  // Add edges between the two nodes, both ways


          }
        }
      }
      edges.toList.distinct // only keep distinct edges
    }
    var edgesList = new ListBuffer[Edge[Int]]()
    for (edge <- getEdges(tokenList, 5)){
      edgesList += Edge(edge._1.get, edge._2.get, 1)
    }
    val edgesRDD : RDD[Edge[Int]]= sc.parallelize(edgesList.toList)


    val nodesFinal  = List.range(0L, nodes.length) zip nodes
    val nodesRDD : RDD[(VertexId, String)] = sc.parallelize(nodesFinal)

    val graph = Graph(nodesRDD, edgesRDD)

    val columns = Seq("term", "rank")
    val results = spark.createDataFrame(graph.pageRank(0.0001, 0.15) // Calculate pagerank
      .vertices.map(node => (nodesFinal(node._1.toInt)._2, node._2)))  // map to get the node (term) strings
      .toDF(columns:_*).sort(col("rank").desc)  // convert to dataframe for displaying results

    nodesFinal.foreach(print)
    graph.inDegrees.collect().foreach(println)
    // Print results
    results.show(10)


  }

}