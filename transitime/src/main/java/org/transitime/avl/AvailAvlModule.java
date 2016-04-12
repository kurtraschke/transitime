/*
 * This file is part of Transitime.org
 *
 * Transitime.org is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License (GPL) as published by
 * the Free Software Foundation, either version 3 of the License, or
 * any later version.
 *
 * Transitime.org is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Transitime.org .  If not, see <http://www.gnu.org/licenses/>.
 */
package org.transitime.avl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.transitime.config.BooleanConfigValue;
import org.transitime.config.EnumConfigValue;
import org.transitime.config.StringConfigValue;
import org.transitime.db.structs.AvlReport;
import org.transitime.db.structs.AvlReport.AssignmentType;
import org.transitime.modules.Module;
import org.transitime.utils.Geo;
import org.transitime.utils.MathUtils;
import org.transitime.utils.Time;

import java.net.URLConnection;

import javax.xml.bind.DatatypeConverter;

/**
 * Reads AVL data from a Avail InfoPoint AVL feed and processes each AVL report.
 *
 * @author SkiBu Smith
 * @author Kurt Raschke <kurt@kurtraschke.com>
 *
 */
public class AvailAvlModule extends XmlPollingAvlModule {

  public static enum AvailAssignmentType {
    BLOCK_ID,
    TRIP_ID,
    ROUTE_ABBREVIATION,
    NONE;
  }
  
  // Parameter that specifies base URL of the Avail feed.
  private static final StringConfigValue availFeedUrl
          = new StringConfigValue("transitime.avl.avail.url",
                  "The URL of the Avail InfoPoint feed to use.");

  private static String getAvailFeedUrl() {
    return availFeedUrl.getValue();
  }

  private static final EnumConfigValue<AvailAssignmentType> assignmentType
          = new EnumConfigValue<>(
                  "transitime.avl.avail.assignmentType",
                  AvailAssignmentType.NONE,
                  "");

  private static final Logger logger
          = LoggerFactory.getLogger(AvailAvlModule.class);

  /**
   * ******************** Member Functions *************************
   */
  /**
   * Constructor
   *
   * @param agencyId
   */
  public AvailAvlModule(String agencyId) {
    super(agencyId);
  }

  /**
   * Feed specific URL to use when accessing data.
   *
   * @return
   */
  @Override
  protected String getUrl() {
    // Determine the URL to use.
    String url = getAvailFeedUrl();
    String endpoint = "/InfoPoint/rest/routes/getvisibleroutes";
    return url + endpoint;
  }
  
  
  @Override
  protected void setRequestHeaders(URLConnection conn) {
    conn.setRequestProperty("Accept", "application/xml");
  }

  /**
   * Extracts the AVL data from the XML document. Uses JDOM to parse the XML
   * because it makes the Java code much simpler.
   *
   * @param doc
   * @return Collection of AvlReports
   * @throws NumberFormatException
   */
  @Override
  protected Collection<AvlReport> extractAvlData(Document doc)
          throws NumberFormatException {
    logger.info("Extracting data from xml file");

    // Get root of doc
    Element rootNode = doc.getRootElement();

    // The return value for the method
    Collection<AvlReport> avlReportsReadIn = new ArrayList<>();

    Namespace ns = Namespace.getNamespace("http://schemas.datacontract.org/2004/07/Availtec.MyAvail.TIDS.DataManager.Models");
    
    List<Element> routes = rootNode.getChildren("Route", ns);

    for (Element route : routes) {
      String routeId = route.getChildText("RouteAbbreviation", ns);
      // Handle getting vehicle location data
      List<Element> vehicles = route.getChild("Vehicles", ns).getChildren("VehicleLocation", ns);
      for (Element vehicle : vehicles) {
        String vehicleId = vehicle.getChildText("VehicleId", ns);
        float lat = Float.parseFloat(vehicle.getChildText("Latitude", ns));
        float lon = Float.parseFloat(vehicle.getChildText("Longitude", ns));

        long gpsEpochTime = DatatypeConverter.parseDateTime(
                vehicle.getChildText("LastUpdated", ns)
        ).getTimeInMillis();

        // Handle the speed
        float speed = Float.NaN;
        String speedStr = vehicle.getChildText("Speed", ns);
        if (speedStr != null && speedStr.length() != 0) {
          //FIXME: what are the units in the Avail feed?!
          speed = Geo.converKmPerHrToMetersPerSecond(Float.parseFloat(speedStr));
        }

        // Handle heading
        float heading = Float.NaN;
        String headingStr = vehicle.getChildText("Heading", ns);
        if (headingStr != null) {
          heading = Float.parseFloat(headingStr);
          // Heading less than 0 means it is invalid
          if (heading < 0) {
            heading = Float.NaN;
          }
        }

        String blockId = vehicle.getChildText("BlockFareboxId", ns);

        String tripId = vehicle.getChildText("TripId", ns);

        // Get driver ID. Be consistent about using null if not set
        // instead of empty string
        String driverId = vehicle.getAttributeValue("DriverName", ns);
        if (driverId != null && driverId.length() == 0) {
          driverId = null;
        }

        // Get passenger count
        Integer passengerCount = null;
        String passengerCountStr = vehicle.getChildText("OnBoard", ns);
        if (passengerCountStr != null) {
          passengerCount = Integer.parseInt(passengerCountStr);
          if (passengerCount < 0) {
            passengerCount = 0;
          }
        }

        // Log raw info for debugging
        logger.debug("vehicleId={} time={} lat={} lon={} spd={} head={} "
                + "blk={} drvr={} psngCnt={}",
                vehicleId, Time.timeStrMsec(gpsEpochTime), lat, lon, speed,
                heading, blockId, driverId, passengerCount);

        // Create the AVL object and send it to the JMS topic.
        AvlReport avlReport = new AvlReport(vehicleId, gpsEpochTime,
                MathUtils.round(lat, 5), MathUtils.round(lon, 5),
                speed, heading, "Avail", null, driverId,
                null, // license plate
                passengerCount,
                Float.NaN); // passengerFullness

        System.out.println("****" + assignmentType.toString());
        
        // Record the assignment for the vehicle if it is available
        switch(assignmentType.getValue()) {
          case TRIP_ID:
            avlReport.setAssignment(blockId, AssignmentType.BLOCK_ID);
            break;
          case BLOCK_ID:
            avlReport.setAssignment(tripId, AssignmentType.TRIP_ID);
            break;
          case ROUTE_ABBREVIATION:
            avlReport.setAssignment(routeId, AssignmentType.ROUTE_ID);
            break;
          case NONE:
          default:
            avlReport.setAssignment(null, AssignmentType.UNSET);
            break;
        }

        avlReportsReadIn.add(avlReport);
      }
    }
    return avlReportsReadIn;
  }

  /**
   * Just for debugging
   *
   * @param args
   */
  public static void main(String[] args) {
    // Create an AvailAvlModule for testing
    Module.start("org.transitime.avl.AvailAvlModule");
  }

}
