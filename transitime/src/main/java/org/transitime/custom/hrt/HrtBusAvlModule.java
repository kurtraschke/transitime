/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.transitime.custom.hrt;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.transitime.avl.PollUrlAvlModule;
import org.transitime.config.StringConfigValue;
import org.transitime.db.structs.AvlReport;
import org.transitime.db.structs.Location;
import org.transitime.modules.Module;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

/**
 *
 * @author kurt
 */
public class HrtBusAvlModule extends PollUrlAvlModule {

  private static final Logger logger
          = LoggerFactory.getLogger(HrtBusAvlModule.class);

  private static final StringConfigValue HrtBusFeedEndpoint = new StringConfigValue("hrtBusFeedEndpoint",
          "ftp://216.54.15.3/Anrd/hrtrtf.txt",
          "HRT CSV bus feed endpoint");

  public HrtBusAvlModule(String agencyId) {
    super(agencyId);
  }

  @Override
  protected String getUrl() {
    return HrtBusFeedEndpoint.getValue();
  }

  @Override
  protected void processData(InputStream in) throws Exception {
    CsvParserSettings settings = new CsvParserSettings();
    settings.setHeaders("time", "date", "vehicleId", "position", "positionValid", "adherence", "adherenceValid", "route", "direction", "stop");
    settings.setHeaderExtractionEnabled(true);
    CsvParser parser = new CsvParser(settings);

    try (InputStreamReader isr = new InputStreamReader(in)) {
      List<String[]> allRows = parser.parseAll(isr);

      for (String[] row : allRows) {
        try {
          Bus bus = new Bus(row);

          if (bus.isLocationValid() && !bus.getVehicleId().equals("null")) {
            AvlReport busAvlReport = bus.makeAvlReport();
            processAvlReport(busAvlReport);
          }

        } catch (Exception e) {
          logger.warn("Exception processing bus row", e);
          logger.warn(Arrays.toString(row));
        }
      }
    }
  }

  /**
   * For testing
   *
   * @param args
   */
  public static void main(String[] args) {
    // Create an HrtBusAvlModule for testing
    Module.start("org.transitime.custom.hrt.HrtBusAvlModule");
  }

  private static class Bus {

    private final Date timestamp;
    private final String vehicleId;
    private final Location location;
    private final boolean locationValid;
    private final int adherence;
    private final boolean adherenceValid;
    private final String route;
    private final String direction;
    private final String stop;

    private static Date makeTimestamp(String time, String date) {
      TimeZone tz = TimeZone.getTimeZone("US/Eastern");
      String[] dateParts = date.split("/");
      String[] timeParts = time.split(":");

      int year = new GregorianCalendar(tz).get(Calendar.YEAR);
      int month = Integer.parseInt(dateParts[0], 10);
      int day = Integer.parseInt(dateParts[1], 10);
      int hour = Integer.parseInt(timeParts[0], 10);
      int minute = Integer.parseInt(timeParts[1], 10);
      int second = Integer.parseInt(timeParts[2], 10);

      Calendar cal = new GregorianCalendar(tz);
      cal.clear();
      cal.set(year, month - 1, day, hour, minute, second);

      return cal.getTime();
    }

    private static Location makeLocation(String rawPosition) {
      String[] positionParts = rawPosition.split("/");

      return new Location(
              (((double) Integer.parseInt(positionParts[0], 10)) / 10000000),
              (((double) Integer.parseInt(positionParts[1], 10)) / 10000000)
      );
    }

    public Bus(String[] csvColumns) {
      this.timestamp = makeTimestamp(csvColumns[0], csvColumns[1]);
      this.vehicleId = csvColumns[2];
      this.location = makeLocation(csvColumns[3]);
      this.locationValid = (csvColumns[4].equals("V"));
      this.adherence = Integer.parseInt(csvColumns[5], 10);
      this.adherenceValid = (csvColumns[6].equals("V"));

      if (csvColumns.length == 10) {
        this.route = String.format("%03d", Integer.parseInt(csvColumns[7]));;
        this.direction = csvColumns[7];
        this.stop = csvColumns[7];
      } else {
        this.route = null;
        this.direction = null;
        this.stop = null;
      }
    }

    public AvlReport makeAvlReport() {
      AvlReport avlReport = new AvlReport(vehicleId, timestamp.getTime(), location);
      if (route != null) {
        avlReport.setAssignment(route, AvlReport.AssignmentType.ROUTE_ID);
      }
      return avlReport;
    }

    public Date getTimestamp() {
      return timestamp;
    }

    public String getVehicleId() {
      return vehicleId;
    }

    public Location getLocation() {
      return location;
    }

    public boolean isLocationValid() {
      return locationValid;
    }

    public int getAdherence() {
      return adherence;
    }

    public boolean isAdherenceValid() {
      return adherenceValid;
    }

    public String getRoute() {
      return route;
    }

    public String getDirection() {
      return direction;
    }

    public String getStop() {
      return stop;
    }

  }
}
