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
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

@Tags({"ASN1Processor"})
@CapabilityDescription("Extract ASN.1 binary file to CSV records.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class ASN1Processor extends AbstractProcessor {
    static Logger logger = Logger.getLogger("com.bonyansystem");
    public static final PropertyDescriptor CSV_SCHEMA = new PropertyDescriptor
            .Builder().name("CSV_SCHEMA")
            .displayName("CSV Schema")
            .description("Comma separated values the resembles CSV schema. Fixed values (INTEGER type): \n REC_NO: file record number. SUB_SEQ: ASN.1 records seuqnce.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_TYPES = new PropertyDescriptor
            .Builder().name("DATA_TYPES")
            .displayName("Data Types")
            .description("Comma separated data types: TBCD_STRING, OCTET_STRING, IA5_STRING, IP_STRING, INTEGER, IPV6_STRING.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor
            .Builder().name("BUFFER_SIZE")
            .displayName("Buffer Size")
            .description("Disk read/write buffer size in kilobytes. Default=4")
            .required(false)
            .allowableValues("1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024")
            .defaultValue("4")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor
            .Builder().name("LOG_LEVEL")
            .displayName("Logging")
            .description("ASN.1 decoding logging. True/False")
            .required(false)
            .allowableValues("ALL", "INFO", "OFF")
            .defaultValue("INFO")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success relationship.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("ASN.1 parse error relationship.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(CSV_SCHEMA);
        descriptors.add(DATA_TYPES);
        descriptors.add(BUFFER_SIZE);
        descriptors.add(LOG_LEVEL);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
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
        int recordCount = 0;
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        int bufferSize = context.getProperty(BUFFER_SIZE).asInteger() * 1024;
        FlowFile csvFlowFile = session.create(flowFile);

        switch(context.getProperty(LOG_LEVEL).getValue()){
            case "ALL":
                logger.setLevel(Level.ALL);
                break;
            case "INFO":
                logger.setLevel(Level.INFO);
                break;
            case "OFF":
                logger.setLevel(Level.OFF);
                break;
            default:
                logger.setLevel(Level.INFO);
        }

        BufferedOutputStream bos = new BufferedOutputStream(session.write(csvFlowFile), bufferSize);
        BufferedInputStream bis = new BufferedInputStream(session.read(flowFile), bufferSize);

        ASN1CSVParser p = null;
        try {
            logger.info("Initiating ASN.1 parser.");
            p = new ASN1CSVParser(bis,
                    context.getProperty(CSV_SCHEMA).toString(),
                    context.getProperty(DATA_TYPES).toString());

            recordCount = p.parse(bos);

            bis.close();
            bos.close();

            logger.info("Parse completed. Record Count: " + recordCount);
            csvFlowFile = session.putAttribute(csvFlowFile, "RecordCount", Integer.toString(recordCount));
            session.transfer(csvFlowFile, SUCCESS);
            session.remove(flowFile);

            logger.info("Committing flowfile.");
            session.commit();
            logger.info("Flowfile commit successfull.");
        } catch (Exception e) {
            logger.severe("ASN.1 Error while parsing.");
            session.transfer(flowFile, FAILURE);
            session.remove(csvFlowFile);
            session.commit();
            throw new ProcessException(e.getCause());
        } finally{
            try {
                bis.close();
                bos.close();
            } catch (IOException e){
                throw new ProcessException(e.getCause());
            }
        }

    }
}


