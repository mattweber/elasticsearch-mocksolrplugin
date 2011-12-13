/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package co.diji.solr;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.XML;

import java.io.Writer;
import java.io.IOException;
import java.util.*;

import org.apache.lucene.document.Fieldable;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;

/**
 * Writes objects to xml.  This class is taken directly out of the 
 * Solr source code and modified to remove the stuff we do not need
 * for the plugin.
 *
 */
final public class XMLWriter {

  //
  // static thread safe part
  //
  public static final char[] XML_START1="<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n".toCharArray();
  public static final char[] XML_START2_NOSCHEMA=(
  "<response>\n"
          ).toCharArray();

  ////////////////////////////////////////////////////////////
  // request instance specific (non-static, not shared between threads)
  ////////////////////////////////////////////////////////////

  private final Writer writer;

  private final DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
  
  public XMLWriter(Writer writer) {
    this.writer = writer;
  }

  /** Writes the XML attribute name/val. A null val means that the attribute is missing. */
  public void writeAttr(String name, String val) throws IOException {
    writeAttr(name, val, true);
  }

  public void writeAttr(String name, String val, boolean escape) throws IOException{
    if (val != null) {
      writer.write(' ');
      writer.write(name);
      writer.write("=\"");
      if(escape){
        XML.escapeAttributeValue(val, writer);
      } else {
        writer.write(val);
      }
      writer.write('"');
    }
  }

  /**Writes a tag with attributes
   *
   * @param tag
   * @param attributes
   * @param closeTag
   * @param escape
   * @throws IOException
   */
  public void startTag(String tag, Map<String,String> attributes, boolean closeTag, boolean escape) throws IOException {
    writer.write('<');
    writer.write(tag);
    if(!attributes.isEmpty()) {
      for (Map.Entry<String, String> entry : attributes.entrySet()) {
        writeAttr(entry.getKey(), entry.getValue(), escape);
      }
    }
    if (closeTag) {
      writer.write("/>");
    } else {
      writer.write('>');
    }
  }

  /**Write a complete tag w/ attributes and cdata (the cdata is not enclosed in $lt;!CDATA[]!&gt;
   * @param tag
   * @param attributes
   * @param cdata
   * @param escapeCdata
   * @param escapeAttr
   * @throws IOException
   */
  public void writeCdataTag(String tag, Map<String,String> attributes, String cdata, boolean escapeCdata, boolean escapeAttr) throws IOException {
    writer.write('<');
    writer.write(tag);
    if (!attributes.isEmpty()) {
      for (Map.Entry<String, String> entry : attributes.entrySet()) {
        writeAttr(entry.getKey(), entry.getValue(), escapeAttr);
      }
    }
    writer.write('>');
    if (cdata != null && cdata.length() > 0) {
      if (escapeCdata) {
        XML.escapeCharData(cdata, writer);
      } else {
        writer.write(cdata, 0, cdata.length());
      }
    }
    writer.write("</");
    writer.write(tag);
    writer.write('>');
  }



  public void startTag(String tag, String name, boolean closeTag) throws IOException {
    writer.write('<');
    writer.write(tag);
    if (name!=null) {
      writeAttr("name", name);
      if (closeTag) {
        writer.write("/>");
      } else {
        writer.write(">");
      }
    } else {
      if (closeTag) {
        writer.write("/>");
      } else {
        writer.write('>');
      }
    }
  }

  private static final Comparator fieldnameComparator = new Comparator() {
    public int compare(Object o, Object o1) {
      Fieldable f1 = (Fieldable)o; Fieldable f2 = (Fieldable)o1;
      int cmp = f1.name().compareTo(f2.name());
      return cmp;
      // note - the sort is stable, so this should not have affected the ordering
      // of fields with the same name w.r.t eachother.
    }
  };


  /**
   * @since solr 1.3
   */
  final void writeDoc(String name, SolrDocument doc, Set<String> returnFields, boolean includeScore) throws IOException {
    startTag("doc", name, false);

    if (includeScore && returnFields != null ) {
      returnFields.add( "score" );
    }

    for (String fname : doc.getFieldNames()) {
      if (returnFields!=null && !returnFields.contains(fname)) {
        continue;
      }
      Object val = doc.getFieldValue(fname);

      writeVal(fname, val);
    }
    
    writer.write("</doc>");
  }


  private static interface DocumentListInfo {
    Float getMaxScore();
    int getCount();
    long getNumFound();
    long getStart();
    void writeDocs( boolean includeScore, Set<String> fields ) throws IOException;
  }

  private final void writeDocuments(
      String name, 
      DocumentListInfo docs, 
      Set<String> fields) throws IOException 
  {
    boolean includeScore=false;
    if (fields!=null) {
      includeScore = fields.contains("score");
      if (fields.size()==0 || (fields.size()==1 && includeScore) || fields.contains("*")) {
        fields=null;  // null means return all stored fields
      }
    }
    
    int sz=docs.getCount();
    
    writer.write("<result");
    writeAttr("name",name);
    writeAttr("numFound",Long.toString(docs.getNumFound()));  // TODO: change to long
    writeAttr("start",Long.toString(docs.getStart()));        // TODO: change to long
    if (includeScore && docs.getMaxScore()!=null) {
      writeAttr("maxScore",Float.toString(docs.getMaxScore()));
    }
    if (sz==0) {
      writer.write("/>");
      return;
    } else {
      writer.write('>');
    }

    docs.writeDocs(includeScore, fields);

    writer.write("</result>");
  }
  
  public final void writeSolrDocumentList(String name, final SolrDocumentList docs, Set<String> fields) throws IOException 
  {
    this.writeDocuments( name, new DocumentListInfo() 
    {  
      public int getCount() {
        return docs.size();
      }
      
      public Float getMaxScore() {
        return docs.getMaxScore();
      }

      public long getNumFound() {
        return docs.getNumFound();
      }

      public long getStart() {
        return docs.getStart();
      }

      public void writeDocs(boolean includeScore, Set<String> fields) throws IOException {
        for( SolrDocument doc : docs ) {
          writeDoc(null, doc, fields, includeScore);
        }
      }
    }, fields );
  }

  public void writeVal(String name, Object val) throws IOException {

    // if there get to be enough types, perhaps hashing on the type
    // to get a handler might be faster (but types must be exact to do that...)

    // go in order of most common to least common
    if (val==null) {
      writeNull(name);
    } else if (val instanceof String) {
      writeStr(name, (String)val);
    } else if (val instanceof Integer) {
      // it would be slower to pass the int ((Integer)val).intValue()
      writeInt(name, val.toString());
    } else if (val instanceof Boolean) {
      // could be optimized... only two vals
      writeBool(name, val.toString());
    } else if (val instanceof Long) {
      writeLong(name, val.toString());
    } else if (val instanceof Date) {
      writeDate(name,(Date)val);
    } else if (val instanceof Float) {
      // we pass the float instead of using toString() because
      // it may need special formatting. same for double.
      writeFloat(name, ((Float)val).floatValue());
    } else if (val instanceof Double) {
      writeDouble(name, ((Double)val).doubleValue());
    } else if (val instanceof SolrDocumentList) {
        // requires access to IndexReader
      writeSolrDocumentList(name, (SolrDocumentList)val, null);  
    }else if (val instanceof Map) {
      writeMap(name, (Map)val);
    } else if (val instanceof NamedList) {
      writeNamedList(name, (NamedList)val);
    } else if (val instanceof Iterable) {
      writeArray(name,((Iterable)val).iterator());
    } else if (val instanceof Object[]) {
      writeArray(name,(Object[])val);
    } else if (val instanceof Iterator) {
      writeArray(name,(Iterator)val);
    } else {
      // default...
      writeStr(name, val.getClass().getName() + ':' + val.toString());
    }
  }

  //
  // Generic compound types
  //

  public void writeNamedList(String name, NamedList val) throws IOException {
    int sz = val.size();
    startTag("lst", name, sz<=0);

    for (int i=0; i<sz; i++) {
      writeVal(val.getName(i),val.getVal(i));
    }

    if (sz > 0) {
      writer.write("</lst>");
    }
  }

  
  /**
   * writes a Map in the same format as a NamedList, using the
   * stringification of the key Object when it's non-null.
   *
   * @param name
   * @param map
   * @throws IOException
   * @see SolrQueryResponse Note on Returnable Data
   */
  public void writeMap(String name, Map<Object,Object> map) throws IOException {
    int sz = map.size();
    startTag("lst", name, sz<=0);

    for (Map.Entry<Object,Object> entry : map.entrySet()) {
      Object k = entry.getKey();
      Object v = entry.getValue();
      // if (sz<indentThreshold) indent();
      writeVal( null == k ? null : k.toString(), v);
    }

    if (sz > 0) {
      writer.write("</lst>");
    }
  }

  public void writeArray(String name, Object[] val) throws IOException {
    writeArray(name, Arrays.asList(val).iterator());
  }

  public void writeArray(String name, Iterator iter) throws IOException {
    if( iter.hasNext() ) {
      startTag("arr", name, false );

      while( iter.hasNext() ) {
        writeVal(null, iter.next());
      }

      writer.write("</arr>");
    }
    else {
      startTag("arr", name, true );
    }
  }

  //
  // Primitive types
  //

  public void writeNull(String name) throws IOException {
    writePrim("null",name,"",false);
  }

  public void writeStr(String name, String val) throws IOException {
    writePrim("str",name,val,true);
  }

  public void writeInt(String name, String val) throws IOException {
    writePrim("int",name,val,false);
  }

  public void writeInt(String name, int val) throws IOException {
    writeInt(name,Integer.toString(val));
  }

  public void writeLong(String name, String val) throws IOException {
    writePrim("long",name,val,false);
  }

  public void writeLong(String name, long val) throws IOException {
    writeLong(name,Long.toString(val));
  }

  public void writeBool(String name, String val) throws IOException {
    writePrim("bool",name,val,false);
  }

  public void writeBool(String name, boolean val) throws IOException {
    writeBool(name,Boolean.toString(val));
  }

  public void writeShort(String name, String val) throws IOException {
    writePrim("short",name,val,false);
  }

  public void writeShort(String name, short val) throws IOException {
    writeInt(name,Short.toString(val));
  }


  public void writeByte(String name, String val) throws IOException {
    writePrim("byte",name,val,false);
  }

  public void writeByte(String name, byte val) throws IOException {
    writeInt(name,Byte.toString(val));
  }


  public void writeFloat(String name, String val) throws IOException {
    writePrim("float",name,val,false);
  }

  public void writeFloat(String name, float val) throws IOException {
    writeFloat(name,Float.toString(val));
  }

  public void writeDouble(String name, String val) throws IOException {
    writePrim("double",name,val,false);
  }

  public void writeDouble(String name, double val) throws IOException {
    writeDouble(name,Double.toString(val));
  }

  public void writeDate(String name, Date val) throws IOException {
	  // updated to use Joda time
    writeDate(name, new DateTime(val).toString(dateFormat));
  }

  public void writeDate(String name, String val) throws IOException {
    writePrim("date",name,val,false);
  }


  //
  // OPT - specific writeInt, writeFloat, methods might be faster since
  // there would be less write calls (write("<int name=\"" + name + ... + </int>)
  //
  public void writePrim(String tag, String name, String val, boolean escape) throws IOException {
    // OPT - we could use a temp char[] (or a StringBuilder) and if the
    // size was small enough to fit (if escape==false we can calc exact size)
    // then we could put things directly in the temp buf.
    // need to see what percent of CPU this takes up first though...
    // Could test a reusable StringBuilder...

    // is this needed here???
    // Only if a fieldtype calls writeStr or something
    // with a null val instead of calling writeNull
    /***
    if (val==null) {
      if (name==null) writer.write("<null/>");
      else writer.write("<null name=\"" + name + "/>");
    }
    ***/

    int contentLen=val.length();

    startTag(tag, name, contentLen==0);
    if (contentLen==0) return;

    if (escape) {
      XML.escapeCharData(val,writer);
    } else {
      writer.write(val,0,contentLen);
    }

    writer.write("</");
    writer.write(tag);
    writer.write('>');
  }


}