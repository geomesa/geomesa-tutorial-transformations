package geomesa.tutorial;

import com.google.common.base.Joiner;
import org.locationtech.geomesa.core.index.Constants;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.ArrayList;
import java.util.List;

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
public class GdeltFeature {

    public static enum Attributes {

        created_at("Date"),
        country("String"),
        lon("Double"),
        lat("Double"),
        text("String"),
        the_date("String"),
        tweetword("String"),
        the_id("String"),
        MatchCount("Integer"),
        Matches("String"),
        Points("Integer"),
        Avg("Double"),
        PtVar("Double"),
        geom("Point");

        private String type;

        private Attributes(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public String getName() {
            if (name() == "lon") return "long";
            return name();
        }
    }

    /**
     * Builds the feature type for the GDELT data set
     *
     * @param featureName
     * @return
     * @throws SchemaException
     */
    public static SimpleFeatureType buildGdeltFeatureType(String featureName) throws SchemaException {

        List<String> attributes = new ArrayList<String>();
        for (Attributes attribute : Attributes.values()) {
            if (attribute == Attributes.geom) {
                // set geom to be the default geometry for geomesa by adding a *
                attributes.add("*geom:Point:srid=4326");
            } else {
                attributes.add(attribute.name() + ":" + attribute.getType());
            }
        }

        String spec = Joiner.on(",").join(attributes);

        SimpleFeatureType featureType = DataUtilities.createType(featureName, spec);
        //This tells GeoMesa to use this Attribute as the Start Time index
        featureType.getUserData().put(Constants.SF_PROPERTY_START_TIME, Attributes.created_at.name());
        return featureType;
    }
}
