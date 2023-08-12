package org.ergoplatform.uexplorer.storage.kryo

import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import org.ergoplatform.uexplorer.mvstore.{SuperNodeCounter, ValueCodec}

import java.nio.ByteBuffer

object CounterCodec extends ValueCodec[SuperNodeCounter] {
  override def readAll(bytes: Array[Byte]): SuperNodeCounter = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[SuperNodeCounter])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

  override def writeAll(obj: SuperNodeCounter): Array[Byte] = {
    val buffer = ByteBuffer.allocate(64)
    val output = new ByteBufferOutput(buffer)
    val kryo   = KryoSerialization.pool.obtain()
    try kryo.writeObject(output, obj)
    finally {
      KryoSerialization.pool.free(kryo)
      output.close()
    }
    buffer.array()
  }
}
