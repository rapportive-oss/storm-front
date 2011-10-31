package com.rapportive.storm.front;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.Writer;

import java.util.Map;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;

public class TopologyVisualiser {
    private final StormTopology topology;

    public TopologyVisualiser(StormTopology topology) {
        this.topology = topology;
    }

    public void writeDOT(Writer output) {
        printDOT(output, topology);
    }

    private static void printDOT(Writer output, StormTopology topology) {
        final PrintWriter o = new PrintWriter(output);

        o.println("digraph {");

        for (Map.Entry<Integer, SpoutSpec> spoutEntry : topology.get_spouts().entrySet()) {
            printNode(o, spoutEntry.getKey(), attributes(spoutEntry.getValue()));
        }

        for (Map.Entry<Integer, Bolt> boltEntry : topology.get_bolts().entrySet()) {
            final int boltId = boltEntry.getKey();
            final Bolt bolt = boltEntry.getValue();

            printNode(o, boltId, attributes(bolt));

            for (Map.Entry<GlobalStreamId, Grouping> groupingEntry : bolt.get_inputs().entrySet()) {
                printEdge(o, groupingEntry.getKey().get_componentId(), boltId, attributes(groupingEntry.getValue()));
            }
        }

        o.println("}");
        o.close();
    }

    private static void printNode(PrintWriter o, Object nodeId, String attributes) {
        o.print(nodeId);
        if (attributes != null) {
            o.print(" [" + attributes + "]");
        }
        o.println();
    }

    private static void printEdge(PrintWriter o, int srcId, int destId, String attributes) {
        o.print(srcId);
        o.print(" -> ");
        o.print(destId);
        if (attributes != null) {
            o.print(" [" + attributes + "]");
        }
        o.println();
    }

    private static String attributes(SpoutSpec spoutSpec) {
        return "label=\""
            + componentClassLabel(spoutSpec.get_spout_object()) + "\\n"
            + componentStreamsLabel(spoutSpec.get_common())
            + "\", peripheries="
            + componentPeripheries(spoutSpec.get_common());
    }

    private static String attributes(Bolt bolt) {
        return "label=\""
            + componentClassLabel(bolt.get_bolt_object()) + "\\n"
            + componentStreamsLabel(bolt.get_common())
            + "\", peripheries="
            + componentPeripheries(bolt.get_common());
    }

    private static String attributes(Grouping grouping) {
        return "label=\"" + groupingType(grouping) + "\"";
    }

    private static final String componentClassLabel(ComponentObject component) {
        final byte[] serialisedComponent = component.get_serialized_java();
        try {
            final Object topologyComponent = new ObjectInputStream(
                    new ByteArrayInputStream(serialisedComponent))
                .readObject();
            return topologyComponent.getClass().getSimpleName();
        } catch (IOException e) {
            return e.toString().replaceAll("\"", "\\\"");
        } catch (ClassNotFoundException e) {
            return e.toString().replaceAll("\"", "\\\"");
        }
    }

    private static final String componentStreamsLabel(ComponentCommon component) {
        final StringBuilder label = new StringBuilder();
        for (StreamInfo stream : component.get_streams().values()) {
            label
                .append(stream.get_output_fields().toString())
                .append("\\n");
        }
        return label.toString();
    }

    private static final int componentPeripheries(ComponentCommon component) {
        final int parallelism = component.get_parallelism_hint();
        return parallelism < 1 ? 1 : parallelism;
    }

    private static String groupingType(Grouping grouping) {
        if (grouping.is_set_direct()) {
            return "direct";
        } else if (grouping.is_set_all()) {
            return "all";
        } else if (grouping.is_set_none()) {
            return "none";
        } else if (grouping.is_set_shuffle()) {
            return "shuffle";
        } else if (grouping.is_set_fields()) {
            return grouping.get_fields().toString();
        } else {
            return "UNKNOWN";
        }
    }
}
