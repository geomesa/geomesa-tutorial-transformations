package org.geomesa.tutorial;

import geomesa.core.data.AccumuloFeatureStore;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.geotools.data.*;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import java.io.IOException;
import java.util.*;

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
     * Creates a base filter that will return a small subset of our results. This can be tweaked to return different
     * results if desired.
     *
     * @return
     * @throws CQLException
     * @throws IOException
     */
    static Filter createBaseFilter()
            throws CQLException, IOException {

        // Get a FilterFactory2 to build up our query
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

        // We are going to query for events in Ukraine during the
        // civil unrest.

        // We'll start by looking at a particular day in February of 2014
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(Calendar.YEAR, 2014);
        calendar.set(Calendar.MONTH, Calendar.FEBRUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 2);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        Date start = calendar.getTime();

        calendar.set(Calendar.HOUR_OF_DAY, 23);
        Date end = calendar.getTime();

        Filter timeFilter = ff.between(ff.property(GdeltFeature.Attributes.SQLDATE.getName()), ff.literal(start), ff.literal(end));

        // We'll bound our query spatially to Ukraine
        Filter spatialFilter = ff.bbox(GdeltFeature.Attributes.geom.getName(), 22.1371589, 44.386463, 40.228581, 52.379581, "EPSG:4326");

        // we'll also restrict our query to only articles about the US, UK or UN
        Filter attributeFilter = ff.like(ff.property(GdeltFeature.Attributes.Actor1Name.getName()), "UNITED%");

        // Now we can combine our filters using a boolean AND operator
        Filter conjunction = ff.and(Arrays.asList(timeFilter, spatialFilter, attributeFilter));

        return conjunction;
    }

    /**
     * This method executes a basic bounding box query without any transformations.
     *
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
            printResults(iterator, Arrays.asList(GdeltFeature.Attributes.SQLDATE.getName(), GdeltFeature.Attributes.Actor1Name.getName(), GdeltFeature.Attributes.geom.getName()));
        } finally {
            iterator.close();
        }
    }

    /**
     * This method executes a query that transforms the results coming back to say 'hello' to each result.
     *
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
            printResults(iterator, Arrays.asList("derived", "What", "Where"));
        } finally {
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
            printResults(iterator, Arrays.asList("derived", "Where"));
        } finally {
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
     *
     * @param iterator
     * @param attributes
     */
    private static void printResults(FeatureIterator iterator, List<String> attributes) {

        if (!iterator.hasNext()) {
            System.out.println("No results");
        }
        int n = 0;
        while (iterator.hasNext()) {
            Feature feature = iterator.next();
            StringBuilder result = new StringBuilder();
            result.append(++n);
            for (String attribute : attributes) {
                result.append("|").append(attribute).append('=').append(feature.getProperty(attribute).getValue());
            }
            System.out.println(result.toString());
        }
        System.out.println();
    }

    /**
     * Main entry point. Executes queries against an existing GDELT dataset
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // read command line options - this contains the connection to accumulo and the table to query
        CommandLineParser parser = new BasicParser();
        Options options = SetupUtil.getCommonRequiredOptions();
        CommandLine cmd = parser.parse(options, args);

        // verify that we can see this Accumulo destination in a GeoTools manner
        Map<String, String> dsConf = SetupUtil.getAccumuloDataStoreConf(cmd);
        DataStore dataStore = DataStoreFinder.getDataStore(dsConf);
        assert dataStore != null;

        // retrieve the accumulo table being used for logging purposes
        String tableName = dsConf.get(SetupUtil.TABLE_NAME);

        // create the simple feature type for our test
        //TODO allow feature type name to be passed via command line to correspond with gdelt ingestion tutorial
        String simpleFeatureTypeName = "gdelt";
        SimpleFeatureType simpleFeatureType = GdeltFeature.buildGdeltFeatureType(simpleFeatureTypeName);

        // load test data into accumulo

        // TODO add transform without renaming output field
        //TODO add geospatial transform of results

        FeatureStore featureStore = (AccumuloFeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);

        // execute some queries
        withinQuery(simpleFeatureTypeName, featureStore);
//        basicTransformationQuery(simpleFeatureTypeName, featureStore);
//        mutliFieldTransformationQuery(simpleFeatureTypeName, featureStore);
//        subtypeTransformationQuery(simpleFeatureTypeName, featureStore);


    }


}