package org.ergoplatform.uexplorer.storage.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer
import com.esotericsoftware.kryo.serializers.ImmutableCollectionsSerializers.JdkImmutableSetSerializer
import com.esotericsoftware.kryo.serializers.{CollectionSerializer, ImmutableCollectionsSerializers, MapSerializer}
import com.esotericsoftware.kryo.util.Pool
import org.ergoplatform.uexplorer.db.Block
import org.ergoplatform.uexplorer.mvstore.SuperNodeCollector.Counter
import org.ergoplatform.uexplorer.mvstore.ValueCodec
import org.ergoplatform.uexplorer.mvstore.multimap.MultiMapCodec
import org.ergoplatform.uexplorer.storage.kryo.*

import java.nio.ByteBuffer
import java.util
import java.util.{Collections, HashSet, Set}
import scala.util.Try

object KryoSerialization {

  val pool: Pool[Kryo] = new Pool[Kryo](true, false, 8) {
    protected def create: Kryo = {
      val kryo          = new Kryo()
      val mapSerializer = new MapSerializer()
      val setSerializer = new CollectionSerializer[util.Set[AnyRef]]()

      kryo.setRegistrationRequired(true)
      kryo.register(Collections.singleton(null).getClass)
      kryo.register(classOf[util.HashMap[_, _]], mapSerializer)
      kryo.register(classOf[util.HashSet[_]], setSerializer)
      kryo.register(classOf[Counter])
      kryo.register(classOf[Block])
      setSerializer.setAcceptsNull(false)
      setSerializer.setElementsCanBeNull(false)
      mapSerializer.setKeyClass(classOf[String], kryo.getSerializer(classOf[String]))
      mapSerializer.setKeysCanBeNull(false)
      mapSerializer.setValueClass(classOf[java.lang.Long], kryo.getSerializer(classOf[java.lang.Long]))
      mapSerializer.setValuesCanBeNull(false)
      kryo
    }
  }

}
