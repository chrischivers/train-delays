package traindelays.networkrail.movements

import org.scalatest.FlatSpec
import traindelays.networkrail.MockStompClient

class MovementsHandlerTest extends FlatSpec {

  it should "" in {
    val mockStompClient = MockStompClient()
    val listener        = MovementsHandler()
    mockStompClient.client.subscribe("test topic", listener).unsafeRunSync()

    mockStompClient.sendMessage("test topic", "MESSAGE")
  }

}
