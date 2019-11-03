import com.typesafe.config.{Config, ConfigFactory}
import org.junit.Before
import org.junit


class ConfigTest {

  private var mapReduceConfig: Config = null
  @Before def getConfig: Unit = {
    mapReduceConfig = ConfigFactory.load("TagInfo.conf")
  }
  @junit.Test
  def checkFile: Unit = {
    assert(!mapReduceConfig.isEmpty,"Required configurations are present.")
  }

  @junit.Test
  def tagCount: Unit = {
    val start_tags = mapReduceConfig.getString("START_TAGS").split(",")
    val end_tags = mapReduceConfig.getString("END_TAGS").split(",")
    val pub_types = mapReduceConfig.getString("PUBLICATION_TYPES").split(",")

    val result: Boolean = (start_tags.length == end_tags.length && start_tags.length == pub_types.length)
    assert(result)
  }

}
