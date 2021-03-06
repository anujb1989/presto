/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.window;

import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;

public class TestLastValueFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testLastValueUnbounded()
    {
        assertUnboundedWindowQuery("last_value(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", "1993-10-27")
                        .row(5L, "F", "1993-10-27")
                        .row(6L, "F", "1993-10-27")
                        .row(33L, "F", "1993-10-27")
                        .row(1L, "O", "1998-07-21")
                        .row(2L, "O", "1998-07-21")
                        .row(4L, "O", "1998-07-21")
                        .row(7L, "O", "1998-07-21")
                        .row(32L, "O", "1998-07-21")
                        .row(34L, "O", "1998-07-21")
                        .build());
        assertUnboundedWindowQueryWithNulls("last_value(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", "1992-02-21")
                        .row(5L, "F", "1992-02-21")
                        .row(null, "F", "1992-02-21")
                        .row(null, "F", "1992-02-21")
                        .row(34L, "O", "1996-12-01")
                        .row(null, "O", "1996-12-01")
                        .row(1L, null, "1995-07-16")
                        .row(7L, null, "1995-07-16")
                        .row(null, null, "1995-07-16")
                        .row(null, null, "1995-07-16")
                        .build());

        assertUnboundedWindowQuery("last_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 33L)
                        .row(5L, "F", 33L)
                        .row(6L, "F", 33L)
                        .row(33L, "F", 33L)
                        .row(1L, "O", 34L)
                        .row(2L, "O", 34L)
                        .row(4L, "O", 34L)
                        .row(7L, "O", 34L)
                        .row(32L, "O", 34L)
                        .row(34L, "O", 34L)
                        .build());
        assertUnboundedWindowQueryWithNulls("last_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", null)
                        .row(5L, "F", null)
                        .row(null, "F", null)
                        .row(null, "F", null)
                        .row(34L, "O", null)
                        .row(null, "O", null)
                        .row(1L, null, null)
                        .row(7L, null, null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build());
        // Timestamp
        assertWindowQuery("date_format(last_value(cast(orderdate as TIMESTAMP)) OVER (PARTITION BY orderstatus ORDER BY orderkey), '%Y-%m-%d')",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", "1993-10-14")
                        .row(5L, "F", "1994-07-30")
                        .row(6L, "F", "1992-02-21")
                        .row(33L, "F", "1993-10-27")
                        .row(1L, "O", "1996-01-02")
                        .row(2L, "O", "1996-12-01")
                        .row(4L, "O", "1995-10-11")
                        .row(7L, "O", "1996-01-10")
                        .row(32L, "O", "1995-07-16")
                        .row(34L, "O", "1998-07-21")
                        .build());
    }

    @Test
    public void testLastValueBounded()
    {
        assertWindowQuery("last_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 6L)
                        .row(5L, "F", 33L)
                        .row(6L, "F", 33L)
                        .row(33L, "F", 33L)
                        .row(1L, "O", 4L)
                        .row(2L, "O", 7L)
                        .row(4L, "O", 32L)
                        .row(7L, "O", 34L)
                        .row(32L, "O", 34L)
                        .row(34L, "O", 34L)
                        .build());
        assertWindowQueryWithNulls("last_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", null)
                        .row(5L, "F", null)
                        .row(null, "F", null)
                        .row(null, "F", null)
                        .row(34L, "O", null)
                        .row(null, "O", null)
                        .row(1L, null, null)
                        .row(7L, null, null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build());
    }
}
