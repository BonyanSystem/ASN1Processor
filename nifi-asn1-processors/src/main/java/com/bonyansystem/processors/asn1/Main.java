package com.bonyansystem.processors.asn1;

import com.bonyansystem.processors.asn1.ASN1CSVParser;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.logging.*;
/*
Sample properties file:

        #ASN.1 to CSV schema
        #
        #Names are case-sensitive

        SCHEMA=REC_NO,REC_SEQ,79.0,79.5,79.22,79.3,79.13,79.19.2.*.1,79.19.2.*.2,79.19.2.*.5,79.15.1.*.1
        DATA_TYPES=INTEGER,INTEGER,INTEGER,INTEGER,TBCD_STRING,TBCD_STRING,HEX_STRING,INTEGER,INTEGER,OCTET_STRING
 */

public class Main {

    static Logger logger;

    public static void main(String[] args) throws Exception {
        Instant start = Instant.now();
        if(args.length < 1) {
            System.out.println("USAGE: java -jar asn1parser.jar properties_file");
            return;
        }

        if (!new File(args[0]).isFile())
            throw new IOException("Properties file not exists: " + args[0]);

        callParse(args[0]);
        Instant end = Instant.now();
        logger.info("Duration: " + Duration.between(start, end).getSeconds() + "s");
    }

    public static Properties readProperties(String fileName) throws Exception {
        Properties prop = new Properties();
        try {
            //load a properties file from class path, inside static method
            prop.load(new FileInputStream(fileName));
            System.out.println("Properties file: " + fileName);
            //get the property value and print it out
            if(!prop.stringPropertyNames().contains("SCHEMA"))
                throw new Exception("Missing property file item. SCHEMA");
            if(!prop.stringPropertyNames().contains("DATA_TYPES"))
                throw new Exception("Missing property file item. DATA_TYPES");
            if(!prop.stringPropertyNames().contains("INPUT_FILE"))
                throw new Exception("Missing property file item. INPUT_FILE");
            if(!prop.stringPropertyNames().contains("OUTPUT_FILE"))
                throw new Exception("Missing property file item. OUTPUT_FILE");
            if(!prop.stringPropertyNames().contains("LOG_LEVEL"))
                Logger.getGlobal().setLevel(Level.INFO);
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    public static void callParse(String propertiesFile) throws Exception {

        Properties prop = readProperties(propertiesFile);
        File binaryFile = new File(prop.getProperty("INPUT_FILE"));
        InputStream is = new FileInputStream(binaryFile);
        BufferedInputStream bis = new BufferedInputStream(is);

        File csvFile = new File(prop.getProperty("OUTPUT_FILE"));

        initLogging(prop.getProperty("LOG_LEVEL"));

        logger.info("Input file: " + binaryFile.getAbsolutePath());
        logger.info("CSV file: " + csvFile.getAbsolutePath());
        OutputStream os = new FileOutputStream(csvFile, false);
        BufferedOutputStream bos = new BufferedOutputStream(os);

        int recCount = 0;
        try {
            ASN1CSVParser parser =
                    new ASN1CSVParser(bis,
                            prop.getProperty("SCHEMA"),
                            prop.getProperty("DATA_TYPES"));
            recCount += parser.parse(bos);
            logger.info("Total csv record extracted: " + recCount);
            bos.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void initLogging(String logLevel){
        logger = Logger.getLogger("com.bonyansystem");

        switch(logLevel){
            case "ALL":
                logger.setLevel(Level.FINEST);
                break;
            case "INFO":
                logger.setLevel(Level.INFO);
                break;
            case "OFF":
                logger.setLevel(Level.OFF);
                break;
            default:
                logger.warning("Invalid log level.");
                logger.setLevel(Level.INFO);
        }

        logger.info("Set log level to: " + logger.getLevel());
    }
}
