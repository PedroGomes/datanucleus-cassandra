/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

package org.datanucleus.store.cassandra;

import java.io.*;

/**
 * Class for byte to Object (vice-versa) conversion.
 */
public class ConversionUtils {


    public static String ObjectToString(Object object) throws IOException {

        if (object instanceof Integer) {
            int x = (Integer) object;
            return (x + "");
        }
        if (object instanceof Long) {
            long x = (Long) object;
            return (x + "");
        }
        if (object instanceof String) {
            String x = (String) object;
            return x;
        }
        if (object instanceof Float) {
            Float x = (Float) object;
            return (x + "");
        }
        if (object instanceof Double) {
            Double x = (Double) object;
            return (x + "");
        }
        if (object instanceof Byte) {
            Byte x = (Byte) object;
            return (x + "");
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(object);
        String name = new String(bos.toByteArray());
        return name;

    }

    public static <T> T convertBytes(java.lang.Class<T> candidateClass, byte[] data) throws Exception {

        Object result;

        if (candidateClass.equals(Integer.class) || candidateClass.equals(int.class)) {

            if (data == null || data.length != 4) {
                result = 0x0;
            } else {
                result = ( // NOTE: type cast not necessary for int
                        (0xff & data[0]) << 24 |
                                (0xff & data[1]) << 16 |
                                (0xff & data[2]) << 8 |
                                (0xff & data[3])
                );

            }
        } else if (candidateClass.equals(Short.class) || candidateClass.equals(short.class)) {

            if (data == null || data.length != 2) result = (short) 0x0;
            else {
                result = (short) (
                        (0xff & data[0]) << 8 |
                                (0xff & data[1])
                );

            }
        } else if (candidateClass.equals(Character.class) || candidateClass.equals(char.class)) {

            if (data == null || data.length != 2) {
                result = (char) 0x0;
            } else {

                result = (char) (
                        (0xff & data[0]) << 8 |
                                (0xff & data[1])
                );
            }


        } else if (candidateClass.equals(Long.class) || candidateClass.equals(long.class)) {

            if (data == null || data.length != 8) {
                result = (long) 0x0;
            } else {
                result = (
// (Below) convert to longs before shift because digits
// are lost with ints beyond the 32-bit limit
                        (long) (0xff & data[0]) << 56 |
                                (long) (0xff & data[1]) << 48 |
                                (long) (0xff & data[2]) << 40 |
                                (long) (0xff & data[3]) << 32 |
                                (long) (0xff & data[4]) << 24 |
                                (long) (0xff & data[5]) << 16 |
                                (long) (0xff & data[6]) << 8 |
                                (long) (0xff & data[7])
                );

            }


        } else if (candidateClass.equals(Float.class) || candidateClass.equals(float.class)) {

            if (data == null || data.length != 4) {
                result = (float) 0x0;
            } else {
                result = Float.intBitsToFloat((
                        (0xff & data[0]) << 24 |
                                (0xff & data[1]) << 16 |
                                (0xff & data[2]) << 8 |
                                (0xff & data[3])
                ));
            }


        } else if (candidateClass.equals(Double.class) || candidateClass.equals(double.class)) {


            if (data == null || data.length != 8) {
                result = (double) 0x0;
            } else {
                result = Double.longBitsToDouble((long) (0xff & data[0]) << 56 |
                        (long) (0xff & data[1]) << 48 |
                        (long) (0xff & data[2]) << 40 |
                        (long) (0xff & data[3]) << 32 |
                        (long) (0xff & data[4]) << 24 |
                        (long) (0xff & data[5]) << 16 |
                        (long) (0xff & data[6]) << 8 |
                        (long) (0xff & data[7]));
            }

        } else if (candidateClass.equals(Byte.class) || candidateClass.equals(byte.class)) {
            result = (data == null || data.length == 0) ? (byte) 0x0 : data[0];
        } else if (candidateClass.equals(Boolean.class) || candidateClass.equals(boolean.class)) {
            result = (!(data == null || data.length == 0)) && data[0] != 0x00;
        } else if (candidateClass.equals(String.class)) {

            if(data==null)
                result = null;
            else if ( data.length == 0) {
                result = "";
            } else {
                result = new String(data, "UTF-8");
            }

        } else {
            if(data==null){
                result = null;
            }

            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInputStream ois = null;
            try {

                ois = new ObjectInputStream(bis);
                result = ois.readObject();

            } finally {

                if (ois != null) {
                    ois.close();
                }
                bis.close();
            }
        }

        return (T) result;
    }

    public static byte[] convertObject(Object value) throws Exception {

        byte[] result;


        Class<?> candidateClass = value.getClass();

        if (candidateClass.equals(Integer.class) || candidateClass.equals(int.class)) {

            int int_value = (Integer) value;
            result = new byte[]{

                    (byte) (int_value >>> 24),
                    (byte) (int_value >>> 16),
                    (byte) (int_value >>> 8),
                    (byte) int_value};

        } else if (candidateClass.equals(Short.class) || candidateClass.equals(short.class)) {

            short short_value = (Short) value;

            result = new byte[]{
                    (byte) ((short_value >> 8) & 0xff),
                    (byte) ((short_value) & 0xff),
            };
        } else if (candidateClass.equals(Character.class) || candidateClass.equals(char.class)) {

            char char_value = (Character) value;

            result = new byte[]{
                    (byte) ((char_value >> 8) & 0xff),
                    (byte) ((char_value) & 0xff),
            };

        } else if (candidateClass.equals(Long.class) || candidateClass.equals(long.class)) {

            long long_value = (Long) value;
            result = new byte[]{

                    (byte) (long_value >>> 56),
                    (byte) (long_value >>> 48),
                    (byte) (long_value >>> 40),
                    (byte) (long_value >>> 32),
                    (byte) (long_value >>> 24),
                    (byte) (long_value >>> 16),
                    (byte) (long_value >>> 8),
                    (byte) long_value};

        } else if (candidateClass.equals(String.class)) {
            result = ((String) value).getBytes("UTF-8");
        } else if (candidateClass.equals(Float.class) || candidateClass.equals(float.class)) {
            int raw_float = Float.floatToRawIntBits((Float) value);
            result = new byte[]{

                    (byte) (raw_float >>> 24),
                    (byte) (raw_float >>> 16),
                    (byte) (raw_float >>> 8),
                    (byte) raw_float};

        } else if (candidateClass.equals(Double.class) || candidateClass.equals(double.class)) {


            long long_value = Double.doubleToRawLongBits((Double) value);
            result = new byte[]{

                    (byte) (long_value >>> 56),
                    (byte) (long_value >>> 48),
                    (byte) (long_value >>> 40),
                    (byte) (long_value >>> 32),
                    (byte) (long_value >>> 24),
                    (byte) (long_value >>> 16),
                    (byte) (long_value >>> 8),
                    (byte) long_value};
        } else if (candidateClass.equals(Byte.class) || candidateClass.equals(byte.class)) {
            result = new byte[]{(Byte) value};
        } else if (candidateClass.equals(Boolean.class) || candidateClass.equals(boolean.class)) {
            boolean bool = (Boolean) value;
            if (bool) {
                result = new byte[]{(byte) 1};
            } else {
                result = new byte[]{(byte) 0};
            }
        } else {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(value);
            result = bos.toByteArray();

            oos.close();
            bos.close();
        }

        return result;
    }


}
