/**********************************************************************
 Copyright (c) 2010 Pedro Gomes and Universidade do Minho. All rights reserved.
 (Based on datanucleus-hbase. Copyright (c) 2009 Erik Bengtson and others.)
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 **********************************************************************/

package org.datanucleus.store.cassandra;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;

import java.util.*;

public class CassandraFetchFieldManager extends AbstractFieldManager {

    private AbstractClassMetaData acmd;
    private ObjectProvider objectProvider;

    private Map<String, byte[]> result_map;

    public CassandraFetchFieldManager(AbstractClassMetaData acmd,
                                      ObjectProvider objcp, List<ColumnOrSuperColumn> result) {
        this.acmd = acmd;
        this.objectProvider = objcp;

        result_map = new TreeMap<String, byte[]>();

        for (int index = 0; index < result.size(); index++) {
            ColumnOrSuperColumn columnOrSuperColumn = result.get(index);
            String name = new String(columnOrSuperColumn.getColumn().name);
            result_map.put(name, columnOrSuperColumn.getColumn().value);
        }

    }

    public boolean fetchBooleanField(int fieldNumber) {
        String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);
        Boolean value;
        try {
            byte[] bytes = result_map.get(columnName);
            value = ConversionUtils.convertBytes(Boolean.class, bytes);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public byte fetchByteField(int fieldNumber) {
        String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);
        Byte value;
        try {
            byte[] bytes = result_map.get(columnName);
            value = ConversionUtils.convertBytes(Byte.class, bytes);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public char fetchCharField(int fieldNumber) {
        String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);
        Character value;
        try {
            byte[] bytes = result_map.get(columnName);
            value = ConversionUtils.convertBytes(Character.class, bytes);
        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public double fetchDoubleField(int fieldNumber) {
        String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);
        Double value;
        try {
            byte[] bytes = result_map.get(columnName);
            value = ConversionUtils.convertBytes(Double.class, bytes);
        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public float fetchFloatField(int fieldNumber) {
        String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

        Float value;
        try {
            byte[] bytes = result_map.get(columnName);
            value = ConversionUtils.convertBytes(Float.class, bytes);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public int fetchIntField(int fieldNumber) {
        String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

        int value;
        try {
            byte[] bytes = result_map.get(columnName);
            value = ConversionUtils.convertBytes(Integer.class, bytes);
        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public long fetchLongField(int fieldNumber) {
        String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

        long value;
        try {
            byte[] bytes = result_map.get(columnName);
            value = ConversionUtils.convertBytes(Long.class, bytes);
        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public short fetchShortField(int fieldNumber) {
        String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

        short value;
        try {
            byte[] bytes = result_map.get(columnName);
            value = ConversionUtils.convertBytes(Short.class, bytes);
        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public String fetchStringField(int fieldNumber) {
        String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

        String value;
        try {
            byte[] bytes = result_map.get(columnName);
            value = ConversionUtils.convertBytes(String.class, bytes);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }

        return value;
    }

    public Object fetchObjectField(int fieldNumber) {
        String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);
        ExecutionContext context = objectProvider.getExecutionContext();
        ClassLoaderResolver clr = context.getClassLoaderResolver();
        AbstractMemberMetaData fieldMetaData = acmd
                .getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        // get object
        Object value;

        byte[] bytes = result_map.get(columnName);

        if (bytes == null) {
            return null;
        }


        try {

            value = ConversionUtils.convertBytes(fieldMetaData.getType(), bytes);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }

        // handle relations
        int relationType = fieldMetaData.getRelationType(clr);

        if (relationType == Relation.ONE_TO_ONE_BI
                || relationType == Relation.ONE_TO_ONE_UNI
                || relationType == Relation.MANY_TO_ONE_BI) {

            Object id = value;
            String class_name = fieldMetaData.getClassName();
            value = context.findObject(id, true, false, class_name);

        } else if (relationType == Relation.MANY_TO_MANY_BI
                || relationType == Relation.ONE_TO_MANY_BI
                || relationType == Relation.ONE_TO_MANY_UNI) {

            MetaDataManager mmgr = context.getMetaDataManager();

            if (fieldMetaData.hasCollection()) {
                String elementClassName = fieldMetaData.getCollection()
                        .getElementType();

                List<Object> mapping = (List<Object>) value;

                Collection<Object> collection = new ArrayList<Object>(mapping.size());
                for (Object id : mapping) {

                    // System.out.println("Object:"+id.toString());
                    Object element = context.findObject(id, true, false,
                            elementClassName);
                    collection.add(element);
                }

                value = objectProvider.wrapSCOField(fieldNumber, collection,
                        false, false, true);
                //value = collection;
            } else if (fieldMetaData.hasMap()) {
                // Process all keys, values of the Map that are PC

                String key_elementClassName = fieldMetaData.getMap()
                        .getKeyType();
                String value_elementClassName = fieldMetaData.getMap()
                        .getValueType();

                Map<Object, Object> mapping = new TreeMap<Object, Object>();

                Map map = (Map) value;
                ApiAdapter api = context.getApiAdapter();

                Set keys = map.keySet();
                Iterator iter = keys.iterator();
                while (iter.hasNext()) {
                    Object mapKey = iter.next();
                    Object key = null;

                    if (mapKey instanceof javax.jdo.identity.SingleFieldIdentity) {
                        key = context.findObject(mapKey, true, false,
                                key_elementClassName);

                    } else {
                        key = mapKey;
                    }

                    Object mapValue = map.get(key);
                    Object key_value = null;

                    if (mapValue instanceof javax.jdo.identity.SingleFieldIdentity) {

                        key_value = context.findObject(mapValue, true, false,
                                value_elementClassName);
                    } else {
                        key_value = mapValue;
                    }

                    mapping.put(key, key_value);
                }

                //    value = mapping;

                value = objectProvider.wrapSCOField(fieldNumber, mapping, false,
                        false, true);
            }
        }

        return value;
    }

    public Object fetchField(int fieldNumber, Class c) {
        if (c == Boolean.class) {
            return this.fetchBooleanField(fieldNumber);

        }
        if (c == Byte.class) {
            return this.fetchByteField(fieldNumber);

        }
        if (c == Character.class) {
            return this.fetchCharField(fieldNumber);

        }
        if (c == Double.class) {

            return this.fetchDoubleField(fieldNumber);
        }
        if (c == Float.class) {
            return this.fetchFloatField(fieldNumber);

        }
        if (c == Integer.class) {
            return this.fetchIntField(fieldNumber);

        }
        if (c == Long.class) {
            return this.fetchLongField(fieldNumber);

        }
        if (c == Short.class) {
            return this.fetchShortField(fieldNumber);

        }
        if (c == String.class) {
            return this.fetchStringField(fieldNumber);

        }
        if (c == Object.class) {
            return this.fetchObjectField(fieldNumber);

        } else {
            return null;

        }

    }

}
