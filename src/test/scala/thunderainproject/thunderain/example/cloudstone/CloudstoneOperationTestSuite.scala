package thunderainproject.thunderain.example.cloudstone

import thunderainproject.thunderain.framework.Event
import org.apache.spark.streaming.{DStream, TestSuiteBase}


class CloudstoneOperationTestSuite extends TestSuiteBase{

  test("identityOperation") {
    val inputData = Seq(
      Seq(new Event(1234567890, Array(
        ("h_time", "1234567890"),
        ("b_message", "1-1")
      ).toMap),
      new Event(1234567890, Array(
        ("h_time", "1234567890"),
        ("b_message", "1-2")
      ).toMap),
    new Event(1234567890, Array(
      ("h_time", "1234567890"),
      ("b_message", "1-3")
    ).toMap)),
    Seq(new Event(1234567890, Array(
      ("h_time", "1234567890"),
      ("b_message", "1-1")
    ).toMap),
      new Event(1234567890, Array(
        ("h_time", "1234567890"),
        ("b_message", "1-2")
      ).toMap),
      new Event(1234567890, Array(
        ("h_time", "1234567890"),
        ("b_message", "1-3")
      ).toMap)),
      Seq()
    )

    val identityOperator = (stream: DStream[Event]) =>  {
      stream.transform(r => r.coalesce(3, true))

    }

    testOperation(inputData, identityOperator, inputData, true)
  }
}
