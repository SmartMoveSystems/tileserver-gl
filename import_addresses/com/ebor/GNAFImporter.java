package com.ebor;


import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

public class GNAFImporter
{
   private static Logger log = Logger.getLogger(GNAFImporter.class.getName());

   public GNAFImporter()
   {
   }
   
   public static void main(String[] args)
   {
      //importGNAF();
//      importOpenAddress();
//      getCSVFiles("C://aa//usa//");
      importOpenAddressGEO("C:\\aa\\OSM\\au_countrywide-addresses-country.geojson", null, false);

      System.exit(0);
   }

   private static List<String> getCSVFiles(String directory)
   {
      List<String> results = new ArrayList<String>();
      try
      {
         Files.walk(Paths.get(directory)).filter(Files::isRegularFile).forEach((f) -> {
            String path = f.toString(); 
            if (path.endsWith(".csv"))
            {
               log.info(path);
               results.add(path);
            }
         });
      }
      catch (Exception e)
      {
         log.log(Level.SEVERE, "Error", e);
      }
      return results;
   }
   
   
   public static void importOpenAddressGEO(String file, String state, boolean test)
   {
      Connection c = null;
      Statement stmt = null;
      StringBuilder buffer = new StringBuilder();

      try
      {
         int invalid = 0;
         int nom = 0;
         int cache = 0;

         BufferedReader reader = new BufferedReader(new FileReader(file));
         log.info("Starting Import");


         int totalAddress = 0;
         Class.forName("org.postgresql.Driver");
         c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/", "openmaptiles", "openmaptiles");
         c.setAutoCommit(false);

         stmt = c.createStatement();

         log.info("Starting Importer");

         String line = reader.readLine();
         int total = 0;
         int count = 0;
         if (line != null) line.replaceAll("\"", "");
         while (line != null)
         {
            try
            {
               JSONObject lineObj = new JSONObject(line);
               JSONObject properties = lineObj.getJSONObject("properties");
               JSONObject geometry = lineObj.getJSONObject("geometry");
               JSONArray coords = null;
               if (geometry != null)
               {
                  coords = geometry.getJSONArray("coordinates");
               }

               if (properties == null || geometry == null || coords == null || coords.length() != 2)
               {
                  line = reader.readLine();
                  continue;
               }

               double longitude = coords.getDouble(0);
               double latitude = coords.getDouble(1);
               if (longitude > 180)
               {
                  longitude = -360 + longitude;
               }

               // Ensure boundaries are appropriate
               if (longitude > 180 || longitude < -180 || latitude > 85 || latitude < -85)
               {
                  line = reader.readLine();
                  continue;
               }

               String text = properties.getString("number");
               text = text == null ? "" : text;
               String text2 = properties.getString("unit");
               text2 = text2 == null ? "" : text2;
               if (text2.startsWith("UNIT"))
               {
                  text = text2.replace("UNIT ", "U") + "/" + text;
               }
               else if (text2.startsWith("APARTMENT"))
               {
                  text = text2.replace("APARTMENT ", "A") + "/" + text;
               }
               else if (!text2.isEmpty())
               {
                  text = text2 + " " + text;
               }
               text = text.replace("'", "''");
               text = text.replace("\\", "\\\\");

               String streetStr = properties.getString("street");;
               streetStr = changeAbbreviations(streetStr);
               String cityStr = properties.getString("city");
               String postcode = properties.getString("postcode");
               String stateString = properties.getString("region");

               long id = 0;
               String idString = properties.getString("id");
               try
               {
                  BigInteger bi = new BigInteger(idString, 16);
               }
               catch(NumberFormatException nfe)
               {
                  idString = "";
               }
               if (idString == null || idString.trim().isEmpty())
               {
                  // Use the hash instead
                  idString = properties.getString("hash");
               }
               if (idString != null && !idString.trim().isEmpty())
               {
                  BigInteger bi = new BigInteger(idString, 16);
                  id = bi.longValue();
               }

//               if (stateString.isEmpty() && countryCode == CountryCode.US)
//               {
//                  int index1 = file.indexOf("\\us\\");
//                  int index2 = file.indexOf("\\", index1 + 4);
//                  stateString = file.substring(index1 + 4, index2).toUpperCase();
//               }

               if (streetStr == null || streetStr.isEmpty())
               {
                  // Skip these as they have no use
                  line = reader.readLine();
                  continue;
               }
               else if (cityStr.isEmpty() || stateString.isEmpty())
               {
//                  // Perform a lookup on the street for this address in nominatim
//                  StreetData data = getStreetData(streetStr, latitude, longitude);
//                  if (data != null && !data.city.isEmpty())
//                  {
//                     if (data.cache)
//                     {
//                        cache++;
//                        totalCache++;
//                     }
//                     else
//                     {
//                        nom++;
//                        totalNom++;
//                     }
//                     cityStr = data.city;
//                     postcode = data.postcode;
//                     stateString = data.state;
//                  }
//                  else
//                  {
                     invalid ++;
                     line = reader.readLine();
                     continue;
//                  }
               }

               String point = "(default, 1, '" + text + "', ST_Transform(ST_SetSRID(ST_MakePoint(" + 
                  longitude + ", " + latitude + "), 4326),3857))";
               if (count > 0) buffer.append(",");
               buffer.append(point);
               total ++;
               count ++;
   
               if (count >= 20000)
               {
                  String sql = "insert into public.osm_housenumber_point values " + buffer.toString() + ";";
                  if (!test)
                  {
                     stmt.executeUpdate(sql);
                     c.commit();
                  }
                  //log.info("SQL: " + sql);
                  log.info("Progress: " + total);
                  count = 0;
                  buffer = new StringBuilder();
                  //break;
               }

               if (total < 10000 && total % 1000 == 0)
               {
//                  log.info("Progress: " + total + " (" + count + ") - invalid " + invalid);
               }
               if (total % 100000 == 0)
               {
                  log.info("Progress: " + total + " (" + total + ") - invalid " + invalid);
               }
            }
            catch(Exception e)
            {
               log.log(Level.WARNING, "Error on line " + total + ": " + line, e);
            }

            line = reader.readLine();
         }

         log.info("Complete: TOTAL " + total + " | INVALID " +
            invalid + " | PERCENT " +
             (total == 0 ? 100 : (Math.round(100.0 * (double)invalid/(double)total))) +
             " | NOM " + nom + " | CACHE " + cache + " - " + file);
         if (count > 0)
         {
            // Write the last batch if it exists
            String sql = "insert into public.osm_housenumber_point values " + buffer.toString() + ";";
            if (!test)
            {
               stmt.executeUpdate(sql);
            }
            //log.info("SQL: " + sql);
            log.info("Complete: " + total + " (" + totalAddress + ")");
         }
         reader.close();
         stmt.close();
         c.commit();
         //c.commit();
         c.close();
      }
      catch (Exception e)
      {
         log.log(Level.SEVERE, "Error reading URL data", e);
      }
   }

   public static void importOpenAddress()
   {
      //insert into public.osm_housenumber_point values(default, 1, '147', ST_Transform(ST_SetSRID(ST_MakePoint(138.56343427, -34.92443982), 4326),3857));
//      doSQL();
//      System.exit(0);
      
      Connection c = null;
      Statement stmt = null;

      try
      {
         int totalAddress = 0;
         Class.forName("org.postgresql.Driver");
         c = DriverManager.getConnection("jdbc:postgresql://localhost:32768/", "openmaptiles", "openmaptiles");
         c.setAutoCommit(false);

         stmt = c.createStatement();
         
         int fileCount = 1;
         List<String> files = getCSVFiles("C://aa//usa//");
         // Loop through all the csv files that can be found
         for (String file: files)
         {
            log.info("Processing file " + fileCount++ + " of " + files.size() + ": " + file);
            BufferedReader reader = new BufferedReader(new FileReader(file));
   
            String line = reader.readLine();
            // Skip the first line as it is a header
            line = reader.readLine();
            StringBuilder buffer = new StringBuilder();
            int count = 0;
            int total = 0;
            if (line != null) line.replaceAll("\"", "");
            while (line != null)
            {
               try
               {
                  total ++;
                  String[] parts = line.split(",");
                  if (parts.length < 5)
                  {
                     line = reader.readLine();
                     continue;
                  }
                  
   //               int postcode = parts[8] != null && !parts[8].isEmpty() ? Integer.parseInt(parts[8]) : 0;
   //               if (postcode != 5031) 
   //               {
   //                  line = reader.readLine();
   //                  continue;
   //               }
                  
                  double longitude = Double.parseDouble(parts[0]);
                  double latitude = Double.parseDouble(parts[1]);
                  // Ensure boundaries are appropriate
                  if (longitude > 180 || longitude < -180 || latitude > 85 || latitude < -85)
                  {
                     line = reader.readLine();
                     continue;
                  }
                  
                  String text = parts[2];
                  String text2 = parts[4];
                  if (text2.startsWith("UNIT"))
                  {
                     text = text2.replace("UNIT ", "U") + "/" + text;
                  }
                  else if (!text2.isEmpty())
                  {
                     text = text2 + " " + text;
                  }
                  text = text.replace("'", "''");
                  text = text.replace("\\", "\\\\");
                  
                  String point = "(default, 1, '" + text + "', ST_Transform(ST_SetSRID(ST_MakePoint(" + 
                     longitude + ", " + latitude + "), 4326),3857))";
                  if (count > 0) buffer.append(",");
                  buffer.append(point);
                  count ++;
      
                  if (count >= 20000)
                  {
                     String sql = "insert into public.osm_housenumber_point values " + buffer.toString() + ";";
                     stmt.executeUpdate(sql);
                     //log.info("SQL: " + sql);
                     log.info("Progress: " + total);
                     count = 0;
                     buffer = new StringBuilder();
                     //break;
                  }
               }
               catch(NumberFormatException nfe)
               {
                  log.log(Level.WARNING, "Number format error: " + nfe.getMessage());
               }
               catch(Exception e)
               {
                  log.log(Level.SEVERE, "Error on line " + count, e);
               }
               
               line = reader.readLine();
            }
   
            totalAddress += total;
            if (count > 0)
            {
               // Write the last batch if it exists
               String sql = "insert into public.osm_housenumber_point values " + buffer.toString() + ";";
               stmt.executeUpdate(sql);
               //log.info("SQL: " + sql);
               log.info("Complete: " + total + " (" + totalAddress + ")");
            }
   
            reader.close();
         }
         stmt.close();
         c.commit();
         //c.commit();
         c.close();
      }
      catch (Exception e)
      {
         log.log(Level.SEVERE, "Error reading URL data", e);
      }
   }

   public static void importGNAF()
   {
      //insert into public.osm_housenumber_point values(default, 1, '147', ST_Transform(ST_SetSRID(ST_MakePoint(138.56343427, -34.92443982), 4326),3857));
//      doSQL();
//      System.exit(0);
      
      Connection c = null;
      Statement stmt = null;

      try
      {
         Class.forName("org.postgresql.Driver");
         c = DriverManager.getConnection("jdbc:postgresql://localhost:32768/", "openmaptiles", "openmaptiles");
         c.setAutoCommit(true);

         stmt = c.createStatement();
         BufferedReader reader = new BufferedReader(new FileReader("G:\\OSM\\Countrywide.csv"));

         String line = reader.readLine();
         // Skip the first line as it is a header
         line = reader.readLine();
         StringBuilder buffer = new StringBuilder();
         int count = 0;
         int total = 0;
         while (line != null)
         {
            try
            {
               total ++;
               String[] parts = line.split(",");
//               int postcode = parts[8] != null && !parts[8].isEmpty() ? Integer.parseInt(parts[8]) : 0;
//               if (postcode != 5031) 
//               {
//                  line = reader.readLine();
//                  continue;
//               }
               
               double longitude = Double.parseDouble(parts[0]);
               double latitude = Double.parseDouble(parts[1]);
               String text = parts[2];
               String text2 = parts[4];
               if (text2.startsWith("UNIT"))
               {
                  text = text2.replace("UNIT ", "U") + "/" + text;
               }
               else if (!text2.isEmpty())
               {
                  text = text2 + " " + text;
               }
               String point = "(default, 1, '" + text + "', ST_Transform(ST_SetSRID(ST_MakePoint(" + 
                  longitude + ", " + latitude + "), 4326),3857))";
               if (count > 0) buffer.append(",");
               buffer.append(point);
               count ++;
   
               if (count >= 20000)
               {
                  String sql = "insert into public.osm_housenumber_point values " + buffer.toString() + ";";
                  stmt.executeUpdate(sql);
                  //log.info("SQL: " + sql);
                  log.info("Progress: " + total);
                  count = 0;
                  buffer = new StringBuilder();
                  //break;
               }
            }
            catch(Exception e)
            {
               log.log(Level.SEVERE, "Error", e);
            }
            
            line = reader.readLine();
         }

         if (count > 0)
         {
            // Write the last batch if it exists
            String sql = "insert into public.osm_housenumber_point values " + buffer.toString() + ";";
            stmt.executeUpdate(sql);
            //log.info("SQL: " + sql);
            log.info("Complete: " + total);
         }

         reader.close();
         stmt.close();
         //c.commit();
         c.close();
      }
      catch (Exception e)
      {
         log.log(Level.SEVERE, "Error reading URL data", e);
      }
   }

   private static void doSQL()
   {
      String sql = "insert into public.osm_housenumber_point values "
         + "(default, 1, '147', ST_Transform(ST_SetSRID(ST_MakePoint(138.56343427, -34.92443982), 4326),3857)),"
         + "(default, 1, '147', ST_Transform(ST_SetSRID(ST_MakePoint(138.56343427, -34.92443982), 4326),3857));";
      
      Connection c = null;
      Statement stmt = null;
      try {
         Class.forName("org.postgresql.Driver");
         c = DriverManager.getConnection("jdbc:postgresql://localhost:32768/", "openmaptiles", "openmaptiles");
         c.setAutoCommit(true);

         stmt = c.createStatement();
         stmt.executeUpdate(sql);
         stmt.executeUpdate(sql);

         stmt.close();
         //c.commit();
         c.close();
      } 
      catch (Exception e) 
      {
         log.warning(e.getClass().getName()+": "+ e.getMessage() );
         System.exit(0);
      }
   }

   private static String changeAbbreviations(String text)
   {
      boolean found = false;
      String result = text;
      if (text.isEmpty()) return result;
      
      String[][] map = {
         {"ST", "STREET"},
         {"RD", "ROAD"},
         {"DR", "DRIVE"},
         {"AVE", "AVENUE"},
         {"CIR", "CIRCUIT"},
         {"CR", "CIRCLE"},
         {"CT", "COURT"},
         {"PL", "PLACE"},
         {"LN", "LANE"},
         {"MTN", "MOUNTAIN"},
         {"HL", "HILL"},
         {"HWY", "HIGHWAY"},
         {"WAY", "WAY"},
         {"E", "EAST"},
         {"W", "WEST"},
         {"S", "SOUTH"},
         {"N", "NORTH"},
         {"HBR", "HARBOR"},
         {"CRES", "CRESCENT"},
         {"CLS", "CLOSE"},
         {"LOOP", "LOOP"},
         {"BLVD", "BOULEVARD"},
         {"TRL", "TRAIL"},
         {"ALY", "ALLEY"},
         {"PKWY", "PARKWAY"},
         {"PKY", "PARKWAY"},
         {"BND", "BEND"},
         {"TER", "TERRACE"},
         {"TRCE", "TERRACE"},
         {"RUN", "RUN"},
         {"EXT", "EXTENSION"},
         {"PATH", "PATH"},
         {"VIS", "VISTA"},
         {"HTS", "HEIGHTS"},
         {"Rd", "Road"},
         {"Dr", "Drive"},
         {"St", "Street"},
         {"Ln", "Lane"},
         {"Ave", "Avenue"},
         {"Pl", "Place"},
         {"Blvd", "Boulevard"},
         {"Hwy", "Highway"},
         {"Ct", "Court"},
         {"Cir", "Circuit"},
      };
      for (String[] item: map)
      {
         if (result.endsWith(" " + item[0]))
         {
            result = result.substring(0, result.length() - item[0].length()) + item[1];
            found = true;
            break; // Assume only one street type
         }
      }
      if (!found)
      {
         //log.info("no change: " + text);
      }
      
      return result;
   }
}
