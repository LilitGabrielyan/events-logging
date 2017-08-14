package events.logging.analytics

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

object FileUtils {
  def succeedStreamDirs(path: String, hadoopConfiguration: Configuration): Array[String] = {
    val fs = FileSystem.get(hadoopConfiguration)
    val paths = new mutable.MutableList[String]()
    if(fs.exists(new org.apache.hadoop.fs.Path(path))) {
	    val listStatus = fs.listStatus(new org.apache.hadoop.fs.Path(path))
	    for (urlStatus <- listStatus) {
		    if (urlStatus.isDirectory) {
			    if (fs.exists(new Path(urlStatus.getPath.toString + "/_SUCCESS"))) {
				    paths += urlStatus.getPath.toString
			    }
		    }
	    }
    }
	  else {
	    println("ERROR: " + path + " doesn't exist!")
    }
    paths.toArray
  }

  def moveProcessedFiles(paths: Array[String], target: String, hadoopConfiguration: Configuration): Unit = {
    val fs = FileSystem.get(hadoopConfiguration)
    for (path <- paths) {
      val source = new Path(path)
      fs.rename(source, new Path(target + "/" + source.getName))
    }
  }
}