package org.ergoplatform.uexplorer.utxo

import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import org.ergoplatform.uexplorer.BlockId

import java.nio.ByteBuffer
import java.util
import org.ergoplatform.uexplorer.utxo.KryoSerialization

trait BlockIdsSerialization {

  protected def serBlockIds(blockIds: java.util.Set[BlockId]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(blockIds.size() * 128)
    val output = new ByteBufferOutput(buffer)
    val kryo   = KryoSerialization.pool.obtain()
    try kryo.writeObject(output, blockIds)
    finally {
      KryoSerialization.pool.free(kryo)
      output.close()
    }
    buffer.array()
  }

  protected def serBlockIdNew(blockId: BlockId): Array[Byte] = {
    val set = new util.HashSet[BlockId]()
    set.add(blockId)
    serBlockIds(set)
  }

  protected def addBlockId(bytes: Array[Byte], blockId: BlockId): Array[Byte] = {
    val set = deserBlockIds(bytes)
    set.add(blockId)
    serBlockIds(set)
  }

  protected def deserBlockIds(bytes: Array[Byte]): java.util.Set[BlockId] = {
    val input = new Input(bytes)
    val kryo  = KryoSerialization.pool.obtain()
    try kryo.readObject(input, classOf[util.HashSet[BlockId]])
    finally {
      KryoSerialization.pool.free(kryo)
      input.close()
    }
  }

}
