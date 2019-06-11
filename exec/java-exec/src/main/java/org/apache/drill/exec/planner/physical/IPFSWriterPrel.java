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
package org.apache.drill.exec.planner.physical;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class IPFSWriterPrel extends WriterPrel {
  private static final List<String> FIELD_NAMES = ImmutableList.of("Fragment", "Number of records written", "Partial Hash");
  public static final String PARTITION_COMPARATOR_FIELD = "P_A_R_T_I_T_I_O_N_C_O_M_P_A_R_A_T_O_R";
  public static final String PARTITION_COMPARATOR_FUNC = "newPartitionValue";

  public IPFSWriterPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, CreateTableEntry createTableEntry) {
    super(cluster, traits, child, createTableEntry);
    setRowType();
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
  public IPFSWriterPrel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IPFSWriterPrel(getCluster(), traitSet, sole(inputs), getCreateTableEntry());
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();
    PhysicalOperator g = getCreateTableEntry().getWriter(child.getPhysicalOperator(creator));
    return creator.addMetadata(this, g);
  }


  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitWriter(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }


  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

}
