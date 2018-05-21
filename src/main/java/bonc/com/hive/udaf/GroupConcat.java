package bonc.com.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @ProjectName: javademo
 * @Package: bonc.com.hive.udaf
 * @ClassName: GroupConcat
 * @Description: 返回分组后 多列变一列聚合输出
 * @Author: Chendeyong
 * @CreateDate: 2018/4/28 11:05
 * @Version: 1.0
 */

@Description(name = "groupconcat", value = "_FUNC_(expr) -返回分组后多列转一列")
public class GroupConcat extends AbstractGenericUDAFResolver {
    private static final Log LOG = LogFactory.getLog(GroupConcat.class.getName());


    @Override
    /**
    * @Description: 参数校验
     * @param info
    * @return: org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
    */
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1) {
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
        //初始输入格式
        private PrimitiveObjectInspector inputOI;
        //输出格式
        private StandardListObjectInspector loi;
        //merge输入格式
        private StandardListObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            if (m == Mode.PARTIAL1) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory
                        .getStandardListObjectInspector(
                                (PrimitiveObjectInspector) ObjectInspectorUtils
                                        .getStandardObjectInspector(inputOI));
            } else {
                if (!(parameters[0] instanceof StandardListObjectInspector)) {
                    inputOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    return (StandardListObjectInspector) ObjectInspectorFactory
                            .getStandardListObjectInspector(inputOI);
                } else {
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputOI = (PrimitiveObjectInspector)
                            internalMergeOI.getListElementObjectInspector();
                    loi = (StandardListObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(internalMergeOI);
                    return loi;
                }
            }
        }

        //定义中间存储类
        static class MkArrayAggregationBuffer implements AggregationBuffer {
            List<Object> container;
        }


        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkArrayAggregationBuffer)agg).container=
                    new ArrayList<Object>();
        }

        private void putIntoList(Object p,MkArrayAggregationBuffer myagg){
            Object pCopy =
                    ObjectInspectorUtils.copyToStandardObject(p,this.inputOI);
            myagg.container.add(pCopy);
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length ==1 );
            Object p = parameters[0];

            if ( p != null ) {
                MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer)agg;
                putIntoList(p,myagg);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer)agg;
            ArrayList<Object> ret =
                    new ArrayList<Object>(myagg.container.size()) ;
            ret.addAll(myagg.container);
            return ret;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer)agg;
            ArrayList<Object> partialResult=
                    (ArrayList<Object>)internalMergeOI.getList(partial);
            for (Object i:partialResult){
                putIntoList(i,myagg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer)agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
            ret.addAll(myagg.container);
            return ret;
        }
    }

}
