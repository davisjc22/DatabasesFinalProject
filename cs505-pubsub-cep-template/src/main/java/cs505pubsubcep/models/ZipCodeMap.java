package cs505pubsubcep.models;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.*;

public class ZipCodeMap {

  public String zipCode;

  public Integer oldValue;

  public Integer newValue;

  public Integer alert;

  public ZipCodeMap(String zipCode, Integer oldValue, Integer newValue, Integer alert) {
    this.zipCode = zipCode;
    this.oldValue = oldValue;
    this.newValue= newValue;
    this.alert = alert;
  }

}
