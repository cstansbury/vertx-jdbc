package io.vertx.ext.jdbc.dialect;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.Dialect;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 
 * @author cstansbury
 */
public class BaseDialect implements Dialect {

  private boolean mSupportsParameterMetaData = true;
  
  private String mDateFormat = "yyyy-MM-dd";
  
  private String mTimestampFormat = "yyyy-MM-dd HH:mm:ss";
  
  @Override
  public void applyBindParams(final PreparedStatement statement, final JsonArray bindParams) throws SQLException {
    for (int i = 0; i < bindParams.size(); i++) {
      final Object value = bindParams.getValue(i);
      final int parameterIndex = i + 1;
      if (value != null) {
        statement.setObject(parameterIndex, value);
      } else if (mSupportsParameterMetaData){
        statement.setNull(parameterIndex, statement.getParameterMetaData().getParameterType(parameterIndex));
      } else {
        statement.setNull(parameterIndex, Types.VARCHAR);
      }
    }
  }
  
  @Override
  public JsonObject parseResultSet(final ResultSet resultSet) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    final JsonObject resultRow = new JsonObject();
    
    for (int i = 0; i < columnCount; i++) {
      final int columnIndex = i + 1;
      final String columnName = metaData.getColumnName(columnIndex).toLowerCase();
      switch (metaData.getColumnType(columnIndex)) {
      case Types.DATE:
        final Date date = resultSet.getDate(columnIndex);
        resultRow.put(columnName, date == null ? null : new SimpleDateFormat(mDateFormat).format(date));
        break;
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        final Date timestamp = resultSet.getTimestamp(columnIndex);
        resultRow.put(columnName, timestamp == null ? null : new SimpleDateFormat(mTimestampFormat).format(timestamp));
        break;
      default:
        resultRow.put(columnName, resultSet.getObject(columnIndex));
      }
    }
    
    return resultRow;
  }
  
}
