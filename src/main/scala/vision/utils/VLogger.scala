package vision.utils

import org.slf4j.LoggerFactory

trait VLogger { me =>
  val log = LoggerFactory.getLogger(me.getClass)
}
