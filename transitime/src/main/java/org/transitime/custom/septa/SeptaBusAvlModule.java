/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.transitime.custom.septa;

import com.google.common.collect.Iterables;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.transitime.avl.PollUrlAvlModule;
import org.transitime.config.StringConfigValue;
import org.transitime.custom.septa.model.Bus;
import org.transitime.db.structs.AvlReport;
import org.transitime.modules.Module;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;

/**
 *
 * @author kurt
 */
public class SeptaBusAvlModule extends PollUrlAvlModule {

  private static final Logger logger
          = LoggerFactory.getLogger(SeptaBusAvlModule.class);

  private static final StringConfigValue septaBusFeedEndpoint = new StringConfigValue("septaBusFeedEndpoint",
          "http://www3.septa.org/hackathon/TransitViewAll/",
          "SEPTA JSON bus feed endpoint");

  public SeptaBusAvlModule(String agencyId) {
    super(agencyId);
  }

  @Override
  protected String getUrl() {
    return septaBusFeedEndpoint.getValue();
  }

  @Override
  protected void processData(InputStream in) throws Exception {
    try (InputStreamReader isr = new InputStreamReader(in)) {

      JsonParser parser = new JsonParser();

      JsonObject root = (JsonObject) parser.parse(isr);

      JsonArray routes = (JsonArray) Iterables.getOnlyElement(root.entrySet()).getValue();

      for (JsonElement routeElement : routes) {
        JsonArray buses = ((JsonArray) (Iterables.getOnlyElement(((JsonObject) routeElement).entrySet()).getValue()));

        for (JsonElement busElement : buses) {
          JsonObject busObject = (JsonObject) busElement;

          try {
            Bus theBus = new Bus(busObject.get("lat").getAsDouble(),
                    busObject.get("lng").getAsDouble(),
                    busObject.get("label").getAsString(),
                    busObject.get("VehicleID").getAsString(), busObject.get(
                            "BlockID").getAsString(),
                    busObject.get("Direction").getAsString(),
                    (!(busObject.get("destination") instanceof JsonNull))
                            ? busObject.get("destination").getAsString() : null,
                    busObject.get("Offset").getAsInt(),
                    busObject.get("Offset_sec").getAsInt()
            );
            AvlReport avlReport = makeAvlReportForBus(theBus);
            processAvlReport(avlReport);

          } catch (Exception e) {
            logger.warn("Exception processing bus JSON", e);
            logger.warn(busObject.toString());
          }
        }
      }
    }
  }

  private AvlReport makeAvlReportForBus(Bus bus) {
    long timestamp = new Date().getTime() - (bus.getOffsetSec() * 1000);
    AvlReport avlReport = new AvlReport(bus.getVehicleId(), timestamp, bus.getLatitude(), bus.getLongitude());
    avlReport.setAssignment(bus.getBlockId(), AvlReport.AssignmentType.BLOCK_ID);
    return avlReport;
  }

  /**
   * For testing
   *
   * @param args
   */
  public static void main(String[] args) {
    // Create a SeptaBusAvlModule for testing
    Module.start("org.transitime.custom.septa.SeptaBusAvlModule");
  }
}
