/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bonyansystem.processors.asn1;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"ASN1Processor"})
@CapabilityDescription("Extract ASN.1 binary file to CSV records.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class ASN1Processor extends AbstractProcessor {

    public static final PropertyDescriptor ITERATION_TAG = new PropertyDescriptor
            .Builder().name("ITERATION_TAG")
            .displayName("Iteration Tag")
            .description("ASN.1 child iteration tag that is used to generate records.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CSV_SCHEMA = new PropertyDescriptor
            .Builder().name("CSV_SCHEMA")
            .displayName("CSV Schema")
            .description("Comma separated values the resembles CSV schema.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_TYPES = new PropertyDescriptor
            .Builder().name("DATA_TYPES")
            .displayName("Data Types")
            .description("Comma separated values the resembles each column data type.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor
            .Builder().name("BUFFER_SIZE")
            .displayName("Buffer Size")
            .description("Disk read/write buffer size in kilobytes. Default=4")
            .required(false)
            .defaultValue("4")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success relationship.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ITERATION_TAG);
        descriptors.add(CSV_SCHEMA);
        descriptors.add(DATA_TYPES);
        descriptors.add(BUFFER_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        int bufferSize = context.getProperty(BUFFER_SIZE).asInteger() * 1024;
        FlowFile csvFlowFile = session.create(flowFile);

        csvFlowFile = session.write(csvFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream outputStream) throws IOException {
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(InputStream inputStream) throws IOException {
                        try {
                            BufferedInputStream bis = new BufferedInputStream(inputStream, bufferSize);
                            BufferedOutputStream bos = new BufferedOutputStream(outputStream, bufferSize);

                            ASN1CSVParser p = new ASN1CSVParser(bis,
                                    context.getProperty(ITERATION_TAG).toString(),
                                    context.getProperty(CSV_SCHEMA).toString(),
                                    context.getProperty(DATA_TYPES).toString());

                            p.parse(bos);
                        } catch (Exception e) {
                            inputStream.close();
                            outputStream.close();

                            throw new ProcessException(e.getMessage());
                        }
                        inputStream.close();
                        outputStream.close();
                    }
                });
            }
        });

        session.transfer(csvFlowFile, SUCCESS);
        session.remove(flowFile);
        session.commit();
    }
}

/*
try {
    ASN1CSVParser p = new ASN1CSVParser(inputStream,
            context.getProperty(ITERATION_TAG).toString(),
            context.getProperty(CSV_SCHEMA).toString(),
            context.getProperty(DATA_TYPES).toString());

    p.parse(outputStream);
} catch (Exception e) {
    throw new ProcessException(e.getMessage());
}

* */