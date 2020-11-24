/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.aggregator.function;

import io.cdap.cdap.api.data.format.StructuredRecord;

/**
 * Peforms selection on two structured record
 */
public interface SelectionFunction {

  /**
   * @return {@link StructuredRecord} that is chosen based on the aggregate function.
   */
  StructuredRecord select(StructuredRecord record1, StructuredRecord record2);
}
