package com.github.charithe.kafka.deserialize;


import com.example.Customer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Arrays;
import java.util.Map;


public class AvroResponseDeserializer implements Deserializer<Customer> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Customer deserialize(String topic, byte[] data) {
        try {
            GenericRecord result = null;

            if (data != null) {
                DatumReader<Customer> reader = new SpecificDatumReader<>(Customer.getClassSchema());

                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                result = reader.read(null, decoder);
            }
            return (Customer) result;
        } catch (Exception ex) {
            throw new SerializationException(
                    "Can't deserialize data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
        }
    }

    @Override
    public void close() {

    }
}