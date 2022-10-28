import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
    if (urlPath.length != 2) {
      return false;
    }

    for (String s : urlPath) {
      if (s.contains(" ")) {
        return false;
      }
    }

    return true;
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    response.setContentType("application/json");
    String urlPath = request.getPathInfo();
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

    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("username");
    factory.setPassword("970422");
    factory.setVirtualHost("/");
    factory.setHost("35.90.145.233");
    factory.setPort(5672);

    Runnable runnable = () -> {
      final Connection conn;
      try {
        conn = factory.newConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(jsonObject.toString());
        String message = "time:" + jsonObject.get("time").getAsString() + ", liftID:" + jsonObject.get("liftID").getAsString();  //TODO
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
        channel.close();
      } catch (TimeoutException | IOException e) {
        e.printStackTrace();
      }
    };

    for (int i = 0; i < 16; i++) {
      Thread sender = new Thread(runnable);
      sender.start();
    }

    ConnectionFactory factory2 = new ConnectionFactory();
    factory2.setUsername("username");
    factory2.setPassword("970422");
    factory2.setVirtualHost("/");
    factory2.setHost("35.90.145.233");
    factory2.setPort(5672);

    Runnable runnable2 = () -> {
      final Connection conn;
      try {
        conn = factory.newConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(jsonObject.toString());
        String message = "time:" + jsonObject.get("time").getAsString() + ", liftID:" + jsonObject.get("liftID").getAsString();  //TODO
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
        channel.close();
      } catch (TimeoutException | IOException e) {
        e.printStackTrace();
      }
    };

    for (int i = 0; i < 16; i++) {
      Thread sender = new Thread(runnable2);
      sender.start();
    }
  }
}
