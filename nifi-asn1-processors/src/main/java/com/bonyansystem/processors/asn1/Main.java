package com.bonyansystem.processors.asn1;

import com.bonyansystem.processors.asn1.ASN1CSVParser;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
/*
Sample properties file:

        #ASN.1 to CSV schema
        #
        #Names are case-sensitive

        ITERATION_TAG=79.19.2.*
        SCHEMA=79.0,79.5,79.22,79.3,79.13,79.19.2.*.1,79.19.2.*.2,79.19.2.*.5,79.15.1.*.1
        DATA_TYPES=INTEGER,INTEGER,TBCD_STRING,TBCD_STRING,HEX_STRING,INTEGER,INTEGER,OCTET_STRING
 */

public class Main {

    static Logger logger = Logger.getGlobal();
    public static void main(String[] args) throws Exception {
        logger.setLevel(Level.FINE);
        Instant start = Instant.now();
        if(args.length < 3) {
            System.out.println("USAGE: java -jar asn1parser.jar properties_file input_file output_file");
            return;
        }
        callParse(args[0], args[1], args[2]);
        Instant end = Instant.now();
        System.out.println("Duration: " + Duration.between(start, end).getSeconds() + "s");
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
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    public static void callParse(String propertiesFile, String inFile, String outFile) throws Exception {

        Properties prop = readProperties(propertiesFile);
        File binaryFile = new File(inFile);
        InputStream is = new FileInputStream(binaryFile);
        BufferedInputStream bis = new BufferedInputStream(is);

        File csvFile = new File(outFile);

        System.out.println("Input file: " + binaryFile.getAbsolutePath());
        System.out.println("CSV file: " + csvFile.getAbsolutePath());
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
            System.err.println(e);
        }
    }
}
