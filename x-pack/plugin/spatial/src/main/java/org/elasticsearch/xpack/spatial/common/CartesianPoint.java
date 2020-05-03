/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentSubParser;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;

import static org.elasticsearch.index.mapper.GeoPointFieldMapper.Names.IGNORE_Z_VALUE;

/**
 * Represents a point in the cartesian space.
 */
public final class CartesianPoint implements ToXContentFragment {

    private static final ParseField X_FIELD = new ParseField("x");
    private static final ParseField Y_FIELD = new ParseField("y");
    private static final ParseField Z_FIELD = new ParseField("z");

    private float x;
    private float y;

    public CartesianPoint() {
    }

    public CartesianPoint(float x, float y) {
        this.x = x;
        this.y = y;
    }

    public CartesianPoint reset(float x, float y) {
        this.x = x;
        this.y = y;
        return this;
    }

    public CartesianPoint resetFromString(String value, final boolean ignoreZValue) {
        if (value.toLowerCase(Locale.ROOT).contains("point")) {
            return resetFromWKT(value, ignoreZValue);
        } else {
            return resetFromCoordinates(value, ignoreZValue);
        }
    }


    public CartesianPoint resetFromCoordinates(String value, final boolean ignoreZValue) {
        String[] vals = value.split(",");
        if (vals.length > 3 || vals.length < 2) {
            throw new ElasticsearchParseException("failed to parse [{}], expected 2 or 3 coordinates "
                + "but found: [{}]", vals, vals.length);
        }
        final float x;
        final float y;
        try {
            x = Float.parseFloat(vals[0].trim());
            if (Float.isFinite(x) == false) {
                throw new ElasticsearchParseException("invalid [{}] value [{}]; " +
                    "must be between -3.4028234663852886E38 and 3.4028234663852886E38",
                    X_FIELD.getPreferredName(),
                    x);
            }
         } catch (NumberFormatException ex) {
            throw new ElasticsearchParseException("[{}]] must be a number", X_FIELD.getPreferredName());
        }
        try {
            y = Float.parseFloat(vals[1].trim());
            if (Float.isFinite(y) == false) {
                throw new ElasticsearchParseException("invalid [{}] value [{}]; " +
                    "must be between -3.4028234663852886E38 and 3.4028234663852886E38",
                    Y_FIELD.getPreferredName(),
                    y);
            }
        } catch (NumberFormatException ex) {
            throw new ElasticsearchParseException("[{}]] must be a number", Y_FIELD.getPreferredName());
        }
        if (vals.length > 2) {
            try {
                CartesianPoint.assertZValue(ignoreZValue, Float.parseFloat(vals[2].trim()));
            } catch (NumberFormatException ex) {
                throw new ElasticsearchParseException("[{}]] must be a number", Y_FIELD.getPreferredName());
            }
        }
        return reset(x, y);
    }

    private CartesianPoint resetFromWKT(String value, boolean ignoreZValue) {
        Geometry geometry;
        try {
            geometry = new WellKnownText(false, new StandardValidator(ignoreZValue))
                .fromWKT(value);
        } catch (Exception e) {
            throw new ElasticsearchParseException("Invalid WKT format", e);
        }
        if (geometry.type() != ShapeType.POINT) {
            throw new ElasticsearchParseException("[{}] supports only POINT among WKT primitives, " +
                "but found {}", PointFieldMapper.CONTENT_TYPE, geometry.type());
        }
        org.elasticsearch.geometry.Point point = (org.elasticsearch.geometry.Point) geometry;
        return reset((float) point.getX(), (float) point.getY());
    }

    public float getX() {
        return this.x;
    }

    public float getY() {
        return this.y;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CartesianPoint point = (CartesianPoint) o;

        if (Float.compare(point.x, x) != 0) return false;
        if (Float.compare(point.y, y) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        int temp;
        temp = x != +0.0f ? Float.floatToIntBits(x) : 0;
        result = Integer.hashCode(temp);
        temp = y != +0.0f ? Float.floatToIntBits(y) : 0;
        result = 31 * result + Integer.hashCode(temp);
        return result;
    }

    @Override
    public String toString() {
        return x + ", " + y;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(X_FIELD.getPreferredName(), x).field(Y_FIELD.getPreferredName(), y).endObject();
    }

    public static CartesianPoint parsePoint(XContentParser parser, CartesianPoint point, boolean ignoreZvalue)
        throws IOException, ElasticsearchParseException {
        float x = Float.NaN;
        float y = Float.NaN;
        NumberFormatException numberFormatException = null;

        if(parser.currentToken() == XContentParser.Token.START_OBJECT) {
            try (XContentSubParser subParser = new XContentSubParser(parser)) {
                while (subParser.nextToken() != XContentParser.Token.END_OBJECT) {
                    if (subParser.currentToken() == XContentParser.Token.FIELD_NAME) {
                        String field = subParser.currentName();
                        if (field.equals(X_FIELD.getPreferredName())) {
                            subParser.nextToken();
                            switch (subParser.currentToken()) {
                                case VALUE_NUMBER:
                                case VALUE_STRING:
                                    try {
                                        x = subParser.floatValue(true);
                                    } catch (NumberFormatException e) {
                                        numberFormatException = e;
                                    }
                                    break;
                                default:
                                    throw new ElasticsearchParseException("[{}] must be a number",
                                        X_FIELD.getPreferredName());
                            }
                        } else if (field.equals(Y_FIELD.getPreferredName())) {
                            subParser.nextToken();
                            switch (subParser.currentToken()) {
                                case VALUE_NUMBER:
                                case VALUE_STRING:
                                    try {
                                        y = subParser.floatValue(true);
                                    } catch (NumberFormatException e) {
                                        numberFormatException = e;
                                    }
                                    break;
                                default:
                                    throw new ElasticsearchParseException("[{}] must be a number",
                                        Y_FIELD.getPreferredName());
                            }
                        } else if (field.equals(Z_FIELD.getPreferredName())) {
                            subParser.nextToken();
                            switch (subParser.currentToken()) {
                                case VALUE_NUMBER:
                                case VALUE_STRING:
                                    try {
                                         CartesianPoint.assertZValue(ignoreZvalue, subParser.floatValue(true));
                                    } catch (NumberFormatException e) {
                                        numberFormatException = e;
                                    }
                                    break;
                                default:
                                    throw new ElasticsearchParseException("[{}] must be a number",
                                        Z_FIELD.getPreferredName());
                            }
                        } else {
                            throw new ElasticsearchParseException("field must be either [{}] or [{}]",
                                X_FIELD.getPreferredName(),
                                Y_FIELD.getPreferredName());
                        }
                    } else {
                        throw new ElasticsearchParseException("token [{}] not allowed", subParser.currentToken());
                    }
                }
            }
           if (numberFormatException != null) {
                throw new ElasticsearchParseException("[{}] and [{}] must be valid float values", numberFormatException,
                    X_FIELD.getPreferredName(),
                    Y_FIELD.getPreferredName());
            } else if (Float.isNaN(x)) {
                throw new ElasticsearchParseException("field [{}] missing", X_FIELD.getPreferredName());
            } else if (Float.isNaN(y)) {
                throw new ElasticsearchParseException("field [{}] missing", Y_FIELD.getPreferredName());
            } else {
                return point.reset(x, y);
            }

        } else if(parser.currentToken() == XContentParser.Token.START_ARRAY) {
            try (XContentSubParser subParser = new XContentSubParser(parser)) {
                int element = 0;
                while (subParser.nextToken() != XContentParser.Token.END_ARRAY) {
                    if (subParser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                        element++;
                        if (element == 1) {
                            x = subParser.floatValue();
                        } else if (element == 2) {
                            y = subParser.floatValue();
                        } else {
                            throw new ElasticsearchParseException("[{}}] field type does not accept > 2 dimensions",
                                PointFieldMapper.CONTENT_TYPE);
                        }
                    } else {
                        throw new ElasticsearchParseException("numeric value expected");
                    }
                }
            }
            return point.reset(x, y);
        } else if(parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            String val = parser.text();
            return point.resetFromString(val, ignoreZvalue);
        } else {
            throw new ElasticsearchParseException("point expected");
        }
    }

    public static CartesianPoint parsePoint(Object value, boolean ignoreZValue) throws ElasticsearchParseException {
        try (XContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            Collections.singletonMap("null_value", value), null)) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            return parsePoint(parser, new CartesianPoint(), ignoreZValue);
        } catch (IOException ex) {
            throw new ElasticsearchParseException("error parsing point", ex);
        }
    }

    public static double assertZValue(final boolean ignoreZValue, float zValue) {
        if (ignoreZValue == false) {
            throw new ElasticsearchParseException("Exception parsing coordinates: found Z value [{}] but [{}] "
                + "parameter is [{}]", zValue, IGNORE_Z_VALUE, ignoreZValue);
        }
        if (Float.isFinite(zValue) == false) {
            throw new ElasticsearchParseException("invalid [{}] value [{}]; " +
                "must be between -3.4028234663852886E38 and 3.4028234663852886E38",
                Z_FIELD.getPreferredName(),
                zValue);
        }
        return zValue;
    }
}
