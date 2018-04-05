package com.hdp.registry;

/**
 * Created by samgupta0 on 4/4/2018.
 */
import java.io.Serializable;

/**
 *
 */
public class TruckEvent implements Serializable {
    Long driverId;
    Long truckId;
    String eventTime;
    String eventType;
    Double longitude;
    Double latitude;
    String eventKey;
    String correlationId;
    String driverName;
    Long routeId;
    String routeName;
    String eventDate;
    Long miles;

    public TruckEvent() {
    }

    public Long getDriverId() {
        return driverId;
    }

    public Long getTruckId() {
        return truckId;
    }

    public String getEventTime() {
        return eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public String getEventKey() {
        return eventKey;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getDriverName() {
        return driverName;
    }

    public Long getRouteId() {
        return routeId;
    }

    public String getRouteName() {
        return routeName;
    }

    public String getEventDate() {
        return eventDate;
    }

    public Long getMiles() {
        return miles;
    }

    public void setMiles(Long miles) {
        this.miles = miles;
    }
}
