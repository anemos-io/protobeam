package io.anemos.protobeam.convert;

import java.util.ArrayList;
import java.util.List;

public class DDL {

    String tableName;
    Struct struct = new Struct();

    public static DDL newBuilder() {
        return new DDL();
    }

    private static void appendIndent(StringBuilder builder, int indent) {
        for (int i = 0; i < indent; i++) {
            builder.append('\t');
        }
    }

    public DDL setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public Struct struct() {
        return this.struct;
    }


    public String toString() {
        StringBuilder builder = new StringBuilder("CREATE TABLE `");
        builder.append(tableName);
        builder.append("` (\n");
        struct.toString(builder, true, 1);
        builder.append(")\n");
        return builder.toString();
    }

    public static class Struct {
        List<Column> columnList = new ArrayList<>();

        public static Struct newBuilder() {
            return new Struct();
        }

        public void addColumn(Column column) {
            columnList.add(column);
        }

        private void toString(StringBuilder builder, boolean topLevel, int indent) {
            if (!topLevel) {
                builder.append("STRUCT<\n");
            }
            for (int ix = 0; ix < columnList.size() - 2; ix++) {
                columnList.get(ix).toString(builder, indent);
                builder.append(",\n");
            }
            columnList.get(columnList.size() - 1).toString(builder, indent);
            builder.append('\n');
            if (!topLevel) {
                appendIndent(builder, indent - 1);
                builder.append(">");
            }
        }
    }

    public static class Column {
        private String name;
        private String description;
        private Struct struct;
        private String typeName;
        private boolean isArray;
        private boolean isNullable;

        public static Column newBuilder() {
            return new Column();
        }

        public DDL.Column setName(String name) {
            this.name = name;
            return this;
        }

        public DDL.Column setDescription(String description) {
            this.description = description;
            return this;
        }

        public void setSchema(Struct struct) {
            this.struct = struct;
        }

        public void setType(String typeName) {
            this.typeName = typeName;
        }

        public void setArray(boolean isArray) {
            this.isArray = isArray;
        }

        public void setNullable(boolean isNullable) {
            this.isNullable = isNullable;
        }

        public void toString(StringBuilder builder, int indent) {
            appendIndent(builder, indent);
            builder.append('`');
            builder.append(name);
            builder.append('`');
            builder.append(' ');
            if (isArray) {
                builder.append("ARRAY<");
                if (struct != null) {
                    builder.append('\n');
                    appendIndent(builder, indent + 1);
                    struct.toString(builder, false, indent + 2);
                    builder.append('\n');
                    appendIndent(builder, indent);
                } else {
                    builder.append(typeName);
                }
                builder.append(">");
            } else {
                if (struct != null) {
                    struct.toString(builder, false, indent + 1);
                } else {
                    builder.append(typeName);
                }

                if (!isNullable) {
                    builder.append(" NOT NULL");
                }
            }


            if (description != null) {
                builder.append(" OPTIONS(description=\"");
                builder.append(description);
                builder.append("\")");
            }
        }
    }

}
