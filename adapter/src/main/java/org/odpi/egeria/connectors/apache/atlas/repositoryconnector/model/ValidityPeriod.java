/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("ValidityPeriod")
public class ValidityPeriod {

    @JsonIgnore private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss zzz");
    @JsonIgnore private static final Logger log = LoggerFactory.getLogger(ValidityPeriod.class);

    private String startTime;
    private String endTime;
    private String timeZone;

    @JsonProperty("startTime") public String getStartTimeAsString() { return this.startTime; }
    @JsonProperty("startTime") public void setStartTimeAsString(String startTime) { this.startTime = startTime; }

    @JsonProperty("endTime") public String getEndTimeAsString() { return this.endTime; }
    @JsonProperty("endTime") public void setEndTimeAsString(String endTime) { this.endTime = endTime; }

    @JsonProperty("timeZone") public String getTimeZone() { return this.timeZone; }
    @JsonProperty("timeZone") public void setTimeZone(String timeZone) { this.timeZone = timeZone; }

    public Date getStartTime() {
        return getDateFromString(getStartTimeAsString() + " " + getTimeZone());
    }

    public Date getEndTime() {
        return getDateFromString(getEndTimeAsString() + " " + getTimeZone());
    }

    private Date getDateFromString(String s) {
        Date d = null;
        try {
            d = dateFormat.parse(s);
        } catch (ParseException e) {
            log.error("Unable to parse date and time from: {}", s);
        }
        return d;
    }

}
