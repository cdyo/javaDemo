package bonc.com.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

@Description(name = "groupconcat", value = "_FUNC_(expr) -返回分组后多列转一列的concat连接")
public class groupLetterSum extends AbstractGenericUDAFResolver {
    private static final Log LOG = LogFactory.getLog(GroupConcat.class.getName());


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1) {
            LOG.error("Exactly one argument is expected.");
            throw new UDFArgumentTypeException(0,
                    "Exactly one argument is expected.");
        }

        ObjectInspector objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info[0]);
        /*ObjectInspector主要作用是解耦数据使用与数据格式
         使得数据流在输入输出端切换不同的输入输出格式
        不同的操作使用不同的格式*/

        if (objectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Argument must be PRIMITIVE, but "
                            + objectInspector.getCategory().name()
                            + " was passed.");
        }

        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;

        if (primitiveObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0,
                    "Argument must be String, but "
                            + primitiveObjectInspector.getPrimitiveCategory().name()
                            + " was passed.");
        }

        return new GroupConcatEvaluator();
        //获取解析器
    }

    public static class GroupConcatEvaluator extends GenericUDAFEvaluator {
        PrimitiveObjectInspector inputObjectInspector;
        ObjectInspector outputInspector;
        PrimitiveObjectInspector intergerObjectInspector;
        int total = 0;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE ) {
                inputObjectInspector = (PrimitiveObjectInspector) parameters[0];
            } else {
                intergerObjectInspector = (PrimitiveObjectInspector) parameters[0];
            }

            System.out.println("current mode :"+m.name()+" init type :"+parameters[0].getTypeName());

            outputInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return outputInspector;
        }

        static class MediaAgg implements AggregationBuffer {
            int sum = 0;

            void add(int num) {
                sum += num;
                //System.out.println("void method "+sum);
            }
        }


        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MediaAgg result = new MediaAgg();
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MediaAgg mediaAgg = new MediaAgg();
        }

        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters[0] != null) {
                MediaAgg mediaAgg = (MediaAgg) agg;
                Object p1 = ((PrimitiveObjectInspector) inputObjectInspector).getPrimitiveJavaObject(parameters[0]);
                mediaAgg.add(String.valueOf(p1).length());
                //System.out.println(parameters[0].toString()+"length :"+String.valueOf(p1).length());
                //System.out.println("mediaAgg"+mediaAgg.sum);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MediaAgg mediaAgg = (MediaAgg) agg;
            total += mediaAgg.sum;
            System.out.println(total);
            return total;

        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                MediaAgg mediaAgg1 = (MediaAgg) agg;
                Integer partialSum = (Integer) intergerObjectInspector.getPrimitiveJavaObject(partial);
                MediaAgg mediaAgg2 = new MediaAgg();
                mediaAgg2.add(partialSum);
                mediaAgg1.add(mediaAgg2.sum);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MediaAgg mediaAgg = (MediaAgg) agg;
            total = mediaAgg.sum;
            return total;
        }
    }

}
