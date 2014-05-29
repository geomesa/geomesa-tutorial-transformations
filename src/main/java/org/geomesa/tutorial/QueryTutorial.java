package org.geomesa.tutorial;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import geomesa.core.data.AccumuloFeatureStore;
import geomesa.core.index.Constants;
import geomesa.utils.text.WKTUtils;
import geomesa.utils.text.WKTUtils$;
import org.apache.commons.cli.*;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Copyright 2014 Commonwealth Computer Research, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class QueryTutorial {

    /**
     * Create a basic simple feature type for testing
     *
     * @param simpleFeatureTypeName
     * @return
     * @throws SchemaException
     */
    static SimpleFeatureType createSimpleFeatureType(String simpleFeatureTypeName)
            throws SchemaException {

        // list the attributes that constitute the feature type
        List<String> attributes = Lists.newArrayList(
                "Who:String",
                "What:java.lang.Long",     // some types require full qualification (see DataUtilities docs)
                "When:Date",               // a date-time field is optional, but can be indexed
                "*Where:Point:srid=4326",  // the "*" denotes the default geometry (used for indexing)
                "Why:String"               // you may have as many other attributes as you like...
        );

        // create the bare simple-feature type
        String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
        SimpleFeatureType simpleFeatureType =
                DataUtilities.createType(simpleFeatureTypeName, simpleFeatureTypeSchema);

        // use the user-data (hints) to specify which date-time field is meant to be indexed;
        // if you skip this step, your data will still be stored, it simply won't be indexed
        simpleFeatureType.getUserData().put(Constants.SF_PROPERTY_START_TIME, "When");

        return simpleFeatureType;
    }

    /**
     * Creates new features. The features will be distributed spatially within a 2-degree box and temporally within a single year.
     * @param simpleFeatureType
     * @param numNewFeatures
     * @return
     */
    static FeatureCollection createNewFeatures(SimpleFeatureType simpleFeatureType, int numNewFeatures) {
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

        String id;
        Object[] NO_VALUES = {};
        String[] PEOPLE_NAMES = {"Adam", "Beth", "Charles", "Diane", "Edgar"};
        Long SECONDS_PER_YEAR = 365L * 24L * 60L * 60L;
        Random random = new Random(5771);
        DateTime MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"));
        Double MIN_X = -78.0;
        Double MIN_Y = -39.0;
        Double DX = 2.0;
        Double DY = 2.0;

        for (int i = 0; i < numNewFeatures; i++) {
            // create the new (unique) identifier and empty feature shell
            id = "Observation." + Integer.toString(i);
            SimpleFeature simpleFeature = SimpleFeatureBuilder.build(simpleFeatureType, NO_VALUES, id);

            // be sure to tell GeoTools explicitly that you want to use the ID you provided
            simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

            // populate the new feature's attributes

            // string value
            simpleFeature.setAttribute("Who", PEOPLE_NAMES[i % PEOPLE_NAMES.length]);

            // long value
            simpleFeature.setAttribute("What", i);

            // location:  construct a random point within a 2-degree-per-side square
            double x = MIN_X + random.nextDouble() * DX;
            double y = MIN_Y + random.nextDouble() * DY;
            Geometry geometry = WKTUtils$.MODULE$.read("POINT(" + x + " " + y + ")");

            // date-time:  construct a random instant within a year
            simpleFeature.setAttribute("Where", geometry);
            DateTime dateTime = MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR));
            simpleFeature.setAttribute("When", dateTime.toDate());

            // another string value
            // we'll leave 'Why' empty for half the features, showing that not all attributes need values
            if (i % 2 == 0) {
                simpleFeature.setAttribute("Why", "reason " + i);
            }

            // accumulate this new feature in the collection
            featureCollection.add(simpleFeature);
        }

        return featureCollection;
    }

    /**
     * Creates a base filter that will return a small subset of our results. This can be tweaked to return different results if desired.
     * @return
     * @throws CQLException
     * @throws IOException
     */
    static Filter createBaseFilter()
            throws CQLException, IOException {

        // there are many different geometric predicates that might be used;
        // here, we just use a bounding-box (BBOX) predicate as an example.
        // this is useful for a rectangular query area
        String cqlGeometry = "BBOX(Where, -77.5, -37.5, -76.5, -36.5)";

        // there are also quite a few temporal predicates; here, we use a
        // "DURING" predicate, because we have a fixed range of times that
        // we want to query
        String cqlDates = "(When DURING 2014-07-01T00:00:00.000Z/2014-09-30T23:59:59.999Z)";

        // there are quite a few predicates that can operate on other attribute
        // types;
        String cqlAttributes = "(Who = 'Beth')";
//        We could alternatively use the GeoTools Filter constant "INCLUDE" which is a default that means
        // to accept everything
        //String cqlAttributes = "INCLUDE"

        String cql = cqlGeometry + " AND " + cqlDates + " AND " + cqlAttributes;
        return CQL.toFilter(cql);
    }

    /**
     * This method executes a basic bounding box query without any transformations.
     * @param simpleFeatureTypeName
     * @param featureSource
     * @throws IOException
     * @throws CQLException
     */
    static void withinQuery(String simpleFeatureTypeName, FeatureSource featureSource) throws IOException, CQLException {

        System.out.println("Submitting within query");

        Filter cqlFilter = createBaseFilter();

        Query query = new Query(simpleFeatureTypeName, cqlFilter);

        FeatureCollection results = featureSource.getFeatures(query);

        FeatureIterator iterator = results.features();

        // loop through all results
        try {
            printResults(iterator, Arrays.asList("Who","What","When","Where","Why"));
        }finally {
            iterator.close();
        }
    }

    /**
     * This method executes a query that transforms the results coming back to say 'hello' to each result.
     * @param simpleFeatureTypeName
     * @param featureSource
     * @throws IOException
     * @throws CQLException
     */
    static void basicTransformationQuery(String simpleFeatureTypeName, FeatureSource featureSource) throws IOException, CQLException

    {
        System.out.println("Submitting basic tranformation query");

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // define the properties that we want returned - this allows us to transform properties using various GeoTools transforms. In this case, we are using a string concatenation to say 'hello' to our results

        String[] properties = new String[]{"derived=strConcat('hello ',Who)", "What", "Where"};

        // create the query - we use the extended constructor to pass in our transform
        Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

        // execute the query
        FeatureCollection results = featureSource.getFeatures(query);

        // loop through all results
        FeatureIterator iterator = results.features();
        try {
            printResults(iterator, Arrays.asList("derived","What","Where"));
        }finally {
            iterator.close();
        }

    }

    static void mutliFieldTransformationQuery(String simpleFeatureTypeName, FeatureSource featureSource) throws IOException, CQLException

    {
        System.out.println("Submitting mutli-field tranformation query");

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // define the properties that we want returned - this allows us to transform properties using various GeoTools transforms. In this case, we are concatenating two different attributes

        String[] properties = new String[]{"derived=strConcat(Who,What)", "Where"};

        // create the query - we use the extended constructor to pass in our transform
        Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

        // execute the query
        FeatureCollection results = featureSource.getFeatures(query);

        // loop through all results
        FeatureIterator iterator = results.features();
        try {
            printResults(iterator, Arrays.asList("derived","Where"));
        }finally {
            iterator.close();
        }
    }

    static void subtypeTransformationQuery(String simpleFeatureTypeName, FeatureSource featureSource)

    {
        //TODO i'm not sure what this is supposed to be doing...
// query a few Features from this table
//        System.out.println("Submitting query");
//        queryFeatures(simpleFeatureTypeName, dataStore,
//                "Where", -77.5, -37.5, -76.5, -36.5,
//                "When", "2014-07-01T00:00:00.000Z", "2014-09-30T23:59:59.999Z",
//                "(Who = 'Bierce')");
//
//        val query = new Query("transformtest", Filter.INCLUDE,
//                Array("name", "geom"))
//
//
//        "name:String,geom:Point:srid=4326" mustEqual DataUtilities.encodeType(results.getSchema)
//        "fid-1=testType|POINT (45 49)" mustEqual DataUtilities.encodeFeature(f)
    }


    /**
     * Iterates through the given iterator and prints out specific attributes for each entry.
     * @param iterator
     * @param attributes
     */
    private static void printResults(FeatureIterator iterator, List<String> attributes) {
        int n = 0;
        while (iterator.hasNext()) {
            Feature feature = iterator.next();
            StringBuilder result = new StringBuilder();
            result.append(++n);
            for (String attribute: attributes) {
                result.append("|").append(attribute).append('=').append(feature.getProperty(attribute).getValue());
            }
            System.out.println(result.toString());
        }
        System.out.println();
    }

    /**
     * Main entry point. Uses common setup in geomesa:geomesa-tutorial-common to read command line arguments specifying
     * accumulo setup.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // read command line options - this contains the connection to accumulo and the table to query
        CommandLineParser parser = new BasicParser();
        Options options = SetupUtil.getCommonRequiredOptions();
        options.addOption(OptionBuilder.create("skipInsert"));
        CommandLine cmd = parser.parse(options, args);

        // verify that we can see this Accumulo destination in a GeoTools manner
        Map<String, String> dsConf = SetupUtil.getAccumuloDataStoreConf(cmd);
        DataStore dataStore = DataStoreFinder.getDataStore(dsConf);
        assert dataStore != null;

        // retrieve the accumulo table being used for logging purposes
        String tableName = dsConf.get(SetupUtil.TABLE_NAME);

        // create the simple feature type for our test
        String simpleFeatureTypeName = "QueryTutorial";
        SimpleFeatureType simpleFeatureType = createSimpleFeatureType(simpleFeatureTypeName);

        // load test data into accumulo

        //TODO use gdelt data
        //TODO add geospatial transform or

        FeatureStore featureStore;
        if (!cmd.hasOption("skipInsert")) {
            // write Feature-specific metadata to the destination table in Accumulo
            // (first creating the table if it does not already exist); you only need
            // to create the FeatureType schema the *first* time you write any Features
            // of this type to the table
            System.out.println("Creating feature-type (schema):  " + simpleFeatureTypeName);
            dataStore.createSchema(simpleFeatureType);

            // create new test features locally, and add them to this table
            System.out.println("Creating new features");
            FeatureCollection featureCollection = createNewFeatures(simpleFeatureType, 1000);
            System.out.println("Inserting new features");
            featureStore = (AccumuloFeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);
            featureStore.addFeatures(featureCollection);
        } else {
            featureStore = (AccumuloFeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);
        }

        // execute some queries
        withinQuery(simpleFeatureTypeName, featureStore);
        basicTransformationQuery(simpleFeatureTypeName, featureStore);
        mutliFieldTransformationQuery(simpleFeatureTypeName, featureStore);
        subtypeTransformationQuery(simpleFeatureTypeName, featureStore);


    }


}