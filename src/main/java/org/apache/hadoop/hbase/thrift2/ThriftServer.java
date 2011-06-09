/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.thrift2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

/**
 * ThriftServer - this class starts up a Thrift server which implements the HBase API specified in the
 * HbaseClient.thrift IDL file.
 */
public class ThriftServer {

  public ThriftServer() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Thrift", null, options,
        "To start the Thrift server run 'bin/hbase-daemon.sh start thrift'\n" +
            "To shutdown the thrift server run 'bin/hbase-daemon.sh stop thrift' or" +
            " send a kill signal to the thrift server pid",
        true);
    System.exit(exitCode);
  }

  private static final String DEFAULT_LISTEN_PORT = "9090";

  /*
   * Start up the Thrift server.
   * @param args
   */
  private static void doMain(String[] args) throws Exception {
    Log log = LogFactory.getLog("ThriftServer");

    Options options = new Options();
    options.addOption("b", "bind", true,
        "Address to bind the Thrift server to. Not supported by the Nonblocking and HsHa server [default: 0.0.0.0]");
    options.addOption("p", "port", true, "Port to bind to [default: 9090]");
    options.addOption("f", "framed", false, "Use framed transport");
    options.addOption("c", "compact", false, "Use the compact protocol");
    options.addOption("h", "help", false, "Print help information");

    OptionGroup servers = new OptionGroup();
    servers.addOption(
        new Option("nonblocking", false, "Use the TNonblockingServer. This implies the framed transport."));
    servers.addOption(new Option("hsha", false, "Use the THsHaServer. This implies the framed transport."));
    servers.addOption(new Option("threadpool", false, "Use the TThreadPoolServer. This is the default."));
    options.addOptionGroup(servers);

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    /**
     * This is so complicated to please both bin/hbase and bin/hbase-daemon. hbase-daemon provides "start" and "stop"
     * arguments hbase should print the help if no argument is provided
     */
    List<String> commandLine = Arrays.asList(args);
    boolean stop = commandLine.contains("stop");
    boolean start = commandLine.contains("start");
    if (cmd.hasOption("help") || !start || stop) {
      printUsageAndExit(options, 1);
    }

    // Get port to bind to
    int listenPort = 0;
    try {
      listenPort = Integer.parseInt(cmd.getOptionValue("port", DEFAULT_LISTEN_PORT));
    } catch (NumberFormatException e) {
      log.error("Could not parse the value provided for the port option", e);
      printUsageAndExit(options, -1);
    }

    // Construct correct ProtocolFactory
    TProtocolFactory protocolFactory;
    if (cmd.hasOption("compact")) {
      log.debug("Using compact protocol");
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      log.debug("Using binary protocol");
      protocolFactory = new TBinaryProtocol.Factory();
    }

    THBaseService.Iface handler = new ThriftHBaseServiceHandler();
    THBaseService.Processor processor = new THBaseService.Processor(handler);

    TServer server;
    if (cmd.hasOption("nonblocking") || cmd.hasOption("hsha")) {
      // TODO: Remove once HBASE-2155 is resolved
      if (cmd.hasOption("bind")) {
        log.error("The Nonblocking and HsHa servers don't support IP address binding at the moment." +
                  " See https://issues.apache.org/jira/browse/HBASE-2155 for details.");
        printUsageAndExit(options, -1);
      }

      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(listenPort);
      TFramedTransport.Factory transportFactory = new TFramedTransport.Factory();

      if (cmd.hasOption("nonblocking")) {
        log.info("starting HBase Nonblocking Thrift server on " + Integer.toString(listenPort));
        TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(transportFactory);
        serverArgs.protocolFactory(protocolFactory);
        server = new TNonblockingServer(serverArgs);
      } else {
        log.info("starting HBase HsHA Thrift server on " + Integer.toString(listenPort));
        THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(transportFactory);
        serverArgs.protocolFactory(protocolFactory);
        server = new THsHaServer(serverArgs);
      }
    } else {
      // Get IP address to bind to
      InetAddress listenAddress = null;
      if (cmd.hasOption("bind")) {
        try {
          listenAddress = InetAddress.getByName(cmd.getOptionValue("bind"));
        } catch (UnknownHostException e) {
          log.error("Could not bind to provided ip address", e);
          printUsageAndExit(options, -1);
        }
      } else {
        listenAddress = InetAddress.getLocalHost();
      }
      TServerTransport serverTransport = new TServerSocket(new InetSocketAddress(listenAddress, listenPort));

      // Construct correct TransportFactory
      TTransportFactory transportFactory;
      if (cmd.hasOption("framed")) {
        transportFactory = new TFramedTransport.Factory();
        log.debug("Using framed transport");
      } else {
        transportFactory = new TTransportFactory();
      }

      log.info("starting HBase ThreadPool Thrift server on " + listenAddress + ":" + Integer.toString(listenPort));
      TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
      serverArgs.processor(processor);
      serverArgs.transportFactory(transportFactory);
      serverArgs.protocolFactory(protocolFactory);
      server = new TThreadPoolServer(serverArgs);
    }

    server.serve();
  }

  public static void main(String[] args) throws Exception {
    doMain(args);
  }
}
