/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.logical;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Writer;
import org.apache.drill.exec.planner.common.DrillWriterRelBase;

import java.util.List;

public class DrillIPFSWriterRel extends DrillWriterRelBase implements DrillRel {
  private static final List<String> FIELD_NAMES = ImmutableList.of("Fragment", "Number of records written", "Partial Hash");
  private final List<Integer> partitionKeys;

  public DrillIPFSWriterRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, CreateTableEntry createTableEntry) {
    super(DRILL_LOGICAL, cluster, traitSet, input, createTableEntry);
    setRowType();
    this.partitionKeys = resolvePartitionKeys();
  }

  @Override
  protected void setRowType() {
    List<RelDataType> fields = Lists.newArrayList();
    fields.add(this.getCluster().getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 255));
    fields.add(this.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT));
    fields.add(this.getCluster().getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 255));
    this.rowType = this.getCluster().getTypeFactory().createStructType(fields, FIELD_NAMES);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillIPFSWriterRel(getCluster(), traitSet, sole(inputs), getCreateTableEntry());
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    LogicalOperator childOp = implementor.visitChild(this, 0, getInput());
    return Writer
        .builder()
        .setInput(childOp)
        .setCreateTableEntry(getCreateTableEntry())
        .build();
  }


  private List<Integer> resolvePartitionKeys(){
    final List<Integer> keys = Lists.newArrayList();
    final RelDataType inputRowType = getInput().getRowType();
    final List<String> partitionCol = getCreateTableEntry().getPartitionColumns();

    for (final String col : partitionCol) {
      final RelDataTypeField field = inputRowType.getField(col, false, false);
      Preconditions.checkArgument(field != null,
          String.format("partition col %s could not be resolved in table's column lists!", col));
      keys.add(field.getIndex());
    }

    return keys;
  }

  public List<Integer> getPartitionKeys() {
    return this.partitionKeys;
  }


}
