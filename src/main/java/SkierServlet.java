import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

@WebServlet(name = "SkierServlet", value = "/skiers/*")
public class SkierServlet extends HttpServlet {
  private final static String QUEUE_NAME = "threadQ";
  private static Connection connection;

  @Override
  public void init() throws ServletException {
    super.init();
    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("username");
    factory.setPassword("970422");
    factory.setVirtualHost("/");
    factory.setHost("54.201.48.169");
    factory.setPort(5672);
    try {
      connection = factory.newConnection();
    } catch (IOException | TimeoutException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    response.setContentType("application/json");
    String urlPath = request.getPathInfo();

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");
    // and now validate url path and return the response status code
    // (and maybe also some value if input is valid)
    if (!isUrlValid(urlParts)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      response.setStatus(HttpServletResponse.SC_OK);
      // do any sophisticated processing with urlParts which contains all the url params
      // TODO: process url params in `urlParts`
      response.getWriter().write("It works!");
    }
  }

  private boolean isUrlValid(String[] urlPath) {
    return urlPath.length == 8;
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    response.setContentType("text/plain");
    String urlPath = request.getPathInfo();
    System.out.println(urlPath);
    String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
    JsonObject jsonObject = JsonParser.parseString(reqBody).getAsJsonObject();
    if (urlPath == null || urlPath.isEmpty()) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");
    // and now validate url path and return the response status code
    // (and maybe also some value if input is valid)
    if (!isUrlValid(urlParts)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      response.setStatus(HttpServletResponse.SC_OK);
      // do any sophisticated processing with urlParts which contains all the url params
      response.getWriter().write("It works!");
    }
    // consumer send messages to rabbitmq

    jsonObject.add("skierID", new JsonPrimitive(urlParts[7]));
    jsonObject.add("dayID", new JsonPrimitive(urlParts[5]));
    jsonObject.add("seasonID", new JsonPrimitive(urlParts[3]));
    jsonObject.add("resortID", new JsonPrimitive(urlParts[1]));

    try {
      Channel channel = connection.createChannel();
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      System.out.println(reqBody);
//        String message = "time:" + jsonObject.get("time").getAsString() + ", liftID:" + jsonObject.get("liftID").getAsString();  //TODO
      channel.basicPublish("", QUEUE_NAME, null, jsonObject.toString().getBytes(StandardCharsets.UTF_8));
      channel.close();
    } catch (TimeoutException | IOException e) {
      e.printStackTrace();
    }
  }
}
