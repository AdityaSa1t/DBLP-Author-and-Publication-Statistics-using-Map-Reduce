import com.typesafe.config.{Config, ConfigFactory}
import mapreduce.{AuthorMapper, PublicationMapper, YearMapper}
import org.junit
import org.junit.Before

class FeatureTest {

  private var mapReduceConfig: Config = null
  val authorMapper = new AuthorMapper
  val yearMapper = new YearMapper
  val publicationMapper = new PublicationMapper
  var TRIAL_XML = new String
  @Before def getConfig: Unit = {
    mapReduceConfig = ConfigFactory.load("TagInfo.conf")
    TRIAL_XML ="<article><author>G</author><author>A</author><title>Title of Test Article.</title><pages>100-200</pages><year>2019</year><booktitle>CS441</booktitle><ee>some.url.one</ee><ee>http://some.url.two</ee><crossref>referenceOne</crossref><url>mainURL</url></article>"
  }
  @junit.Test
  def checkXMLAuthors: Unit = {

    val authorResult = authorMapper.getAuthors(TRIAL_XML)
    assert(authorResult.length == 2)
  }

  @junit.Test
  def checkXMLYear: Unit = {

    val yearResult = yearMapper.getYear(TRIAL_XML)
    assert(yearResult.equals("2019"))
  }

  @junit.Test
  def checkXMLPublicationType: Unit = {

    val pubResult = publicationMapper.getPublications(TRIAL_XML)
    assert(!pubResult.toString.isEmpty)
  }

}
