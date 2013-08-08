<?xml version="1.0" encoding="UTF-8"?>
<!-- This is used to mangle the Finnish translation of MARC21 for Evergreen marc editor. -->

<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>


<xsl:template match="/">
  <xsl:element name="fields">
  <xsl:copy>
    <xsl:apply-templates select="document('data/000.xml')/fields"/>
    <xsl:apply-templates select="document('data/001-006.xml')/fields"/>
    <xsl:apply-templates select="document('data/007.xml')/fields"/>
    <xsl:apply-templates select="document('data/008.xml')/fields"/>
    <xsl:apply-templates select="document('data/01X-04X.xml')/fields"/>
    <xsl:apply-templates select="document('data/05X-08X.xml')/fields"/>
    <xsl:apply-templates select="document('data/1XX.xml')/fields"/>
    <xsl:apply-templates select="document('data/20X-24X.xml')/fields"/>
    <xsl:apply-templates select="document('data/250-270.xml')/fields"/>
    <xsl:apply-templates select="document('data/3XX.xml')/fields"/>
    <xsl:apply-templates select="document('data/4XX.xml')/fields"/>
    <xsl:apply-templates select="document('data/50X-53X.xml')/fields"/>
    <xsl:apply-templates select="document('data/53X-58X.xml')/fields"/>
    <xsl:apply-templates select="document('data/6XX.xml')/fields"/>
    <xsl:apply-templates select="document('data/70X-75X.xml')/fields"/>
    <xsl:apply-templates select="document('data/76X-78X.xml')/fields"/>
    <xsl:apply-templates select="document('data/80X-830.xml')/fields"/>
    <xsl:apply-templates select="document('data/841-88X.xml')/fields"/>
    <xsl:apply-templates select="document('data/9XX.xml')/fields"/>
  </xsl:copy>
  </xsl:element>
</xsl:template>

<xsl:template match="/fields">
    <xsl:for-each select="//datafields/datafield|//controlfields/controlfield[@repeatable!='']">
      <xsl:choose>
        <xsl:when test="contains(./@tag, 'X')">
          <xsl:call-template name="output_field">
            <xsl:with-param name="TAG" select="./@tag"/>
            <xsl:with-param name="REPS">0123456789,0123456789,0123456789</xsl:with-param>
          </xsl:call-template>
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="output_field">
            <xsl:with-param name="TAG" select="./@tag"/>
            <xsl:with-param name="REPS"></xsl:with-param>
          </xsl:call-template>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:for-each>
</xsl:template>


<xsl:template name="output_field">
 <xsl:param name="TAG"/>
 <xsl:param name="REPS"/>
 <xsl:choose>
   <xsl:when test="contains($TAG, 'X') and contains($REPS, '9')">
     <xsl:variable name="REPS_TMP" select="substring-after($REPS, ',')"/>
     <xsl:variable name="REPS_HUNS" select="substring-before($REPS, ',')"/>
     <xsl:variable name="REPS_TENS" select="substring-before($REPS_TMP, ',')"/>
     <xsl:variable name="REPS_ONES" select="substring-after($REPS_TMP, ',')"/>
     <xsl:variable name="OTAG" select="$TAG"/>
     <xsl:choose>
       <xsl:when test="substring($TAG, 3, 1) = 'X' and string-length($REPS_ONES) &gt; 0">
         <xsl:variable name="XTAG" select="concat(substring($TAG, 1, 2), substring($REPS_ONES, 1, 1))"/>
         <xsl:variable name="XREPS_ONES" select="substring($REPS_ONES, 2)"/>
         <xsl:call-template name="output_field">
           <xsl:with-param name="TAG" select="$XTAG"/>
           <xsl:with-param name="REPS" select="concat($REPS_HUNS,',',$REPS_TENS,',')"/>
         </xsl:call-template>
         <xsl:call-template name="output_field">
           <xsl:with-param name="TAG" select="$OTAG"/>
           <xsl:with-param name="REPS" select="concat($REPS_HUNS,',',$REPS_TENS,',',$XREPS_ONES)"/>
         </xsl:call-template>
       </xsl:when>
       <xsl:when test="substring($TAG, 2, 1) = 'X' and string-length($REPS_TENS) &gt; 0">
         <xsl:variable name="XTAG" select="concat(substring($TAG, 1, 1), substring($REPS_TENS, 1, 1), substring($TAG, 3))"/>
         <xsl:variable name="XREPS_TENS" select="substring($REPS_TENS, 2)"/>
         <xsl:call-template name="output_field">
           <xsl:with-param name="TAG" select="$XTAG"/>
           <xsl:with-param name="REPS" select="concat($REPS_HUNS,',',',',$REPS_ONES)"/>
         </xsl:call-template>
         <xsl:call-template name="output_field">
           <xsl:with-param name="TAG" select="$OTAG"/>
           <xsl:with-param name="REPS" select="concat($REPS_HUNS,',',$XREPS_TENS,',',$REPS_ONES)"/>
         </xsl:call-template>
       </xsl:when>
       <xsl:when test="substring($TAG, 1, 1) = 'X' and string-length($REPS_HUNS) &gt; 0">
         <xsl:variable name="XTAG" select="concat(substring($REPS_HUNS, 1, 1), substring($TAG, 2))"/>
         <xsl:variable name="XREPS_HUNS" select="substring($REPS_HUNS, 2)"/>
         <xsl:call-template name="output_field">
           <xsl:with-param name="TAG" select="$XTAG"/>
           <xsl:with-param name="REPS" select="concat(',',$REPS_TENS,',',$REPS_ONES)"/>
         </xsl:call-template>
         <xsl:call-template name="output_field">
           <xsl:with-param name="TAG" select="$OTAG"/>
           <xsl:with-param name="REPS" select="concat($XREPS_HUNS,',',$REPS_TENS,',',$REPS_ONES)"/>
         </xsl:call-template>
       </xsl:when>
     </xsl:choose>
   </xsl:when>
   <xsl:otherwise>
     <xsl:element name="field">
       <xsl:attribute name="tag"><xsl:value-of select="$TAG"/></xsl:attribute>
       <xsl:attribute name="repeatable">
         <xsl:call-template name="mangle_repeatable_YN">
           <xsl:with-param name="REPEATABLE" select="./@repeatable"/>
         </xsl:call-template>
       </xsl:attribute>
       <xsl:element name="name"><xsl:value-of select="./name"/></xsl:element>
       <xsl:element name="description"><xsl:value-of select="./description"/></xsl:element>
       <xsl:call-template name="parse_indicators" />
       <xsl:call-template name="parse_subfields" />
     </xsl:element>
   </xsl:otherwise>
 </xsl:choose>
</xsl:template>

<xsl:template match="description//br">
  <xsl:text>
</xsl:text>
</xsl:template>


<xsl:template name="parse_indicators">
 <xsl:for-each select="./indicators/indicator">
  <xsl:variable name="POS"><xsl:value-of select="./@num"/></xsl:variable>
  <xsl:call-template name="parse_indvalues">
    <xsl:with-param name="POS" select="$POS"/>
  </xsl:call-template>
 </xsl:for-each>
</xsl:template>


<xsl:template name="parse_indvalues">
 <xsl:param name="POS"/>
  <xsl:for-each select="./values/value">
   <xsl:element name="indicator">
   <xsl:attribute name="position"><xsl:value-of select="$POS"/></xsl:attribute>
   <xsl:attribute name="value"><xsl:value-of select="@code"/></xsl:attribute>
   <xsl:element name="description"><xsl:apply-templates select="description"/></xsl:element>
   </xsl:element>
  </xsl:for-each>
</xsl:template>


<xsl:template name="parse_subfields">
 <xsl:for-each select="./subfields/subfield">
   <xsl:choose>
     <xsl:when test="string-length(./@code) = 3">
       <xsl:variable name="ALLCODECHARS">abcdefghijklmnopqrstuvwxyz0123456789</xsl:variable>
       <xsl:variable name="CODECHAR_START"><xsl:value-of select="substring(./@code, 1, 1)"/></xsl:variable>
       <xsl:variable name="CODECHAR_END"><xsl:value-of select="substring(./@code, 3, 1)"/></xsl:variable>
       <xsl:variable name="CODE"><xsl:value-of select="concat($CODECHAR_START, substring-before(substring-after($ALLCODECHARS, $CODECHAR_START), $CODECHAR_END), $CODECHAR_END)"/></xsl:variable>
       <xsl:variable name="REPEATABLE"><xsl:value-of select="./@repeatable"/></xsl:variable>
       <xsl:call-template name="output_subfield">
         <xsl:with-param name="CODE" select="$CODE"/>
         <xsl:with-param name="REPEATABLE" select="$REPEATABLE"/>
       </xsl:call-template>
     </xsl:when>
     <xsl:otherwise>
       <xsl:variable name="CODE"><xsl:value-of select="./@code"/></xsl:variable>
       <xsl:variable name="REPEATABLE"><xsl:value-of select="./@repeatable"/></xsl:variable>
       <xsl:call-template name="output_subfield">
         <xsl:with-param name="CODE" select="$CODE"/>
         <xsl:with-param name="REPEATABLE" select="$REPEATABLE"/>
       </xsl:call-template>
     </xsl:otherwise>
   </xsl:choose>
 </xsl:for-each>
</xsl:template>


<xsl:template name="output_subfield">
 <xsl:param name="CODE"/>
 <xsl:param name="REPEATABLE"/>
 <xsl:choose>
   <xsl:when test="string-length($CODE) &gt; 1">
     <xsl:variable name="FCODE"><xsl:value-of select="$CODE"/></xsl:variable>
     <xsl:call-template name="output_subfield">
       <xsl:with-param name="CODE" select="substring($FCODE, 1, 1)"/>
       <xsl:with-param name="REPEATABLE" select="$REPEATABLE"/>
     </xsl:call-template>
     <xsl:call-template name="output_subfield">
       <xsl:with-param name="CODE" select="substring($FCODE, 2)"/>
       <xsl:with-param name="REPEATABLE" select="$REPEATABLE"/>
     </xsl:call-template>
   </xsl:when>
   <xsl:otherwise>
     <xsl:element name="subfield">
       <xsl:attribute name="code"><xsl:value-of select="$CODE"/></xsl:attribute>
       <xsl:attribute name="repeatable">
         <xsl:call-template name="mangle_repeatable_YN">
           <xsl:with-param name="REPEATABLE" select="$REPEATABLE"/>
         </xsl:call-template>
       </xsl:attribute>
       <xsl:element name="description">
         <xsl:value-of select="name"/><xsl:if test="description">: </xsl:if>
         <xsl:apply-templates select="description"/>
       </xsl:element>
     </xsl:element>
   </xsl:otherwise>
 </xsl:choose>
</xsl:template>

<xsl:template name="mangle_repeatable_YN">
  <xsl:param name="REPEATABLE"/>
  <xsl:choose>
    <xsl:when test="$REPEATABLE = 'Y'"><xsl:text>true</xsl:text></xsl:when>
    <xsl:when test="$REPEATABLE = 'y'"><xsl:text>true</xsl:text></xsl:when>
    <xsl:otherwise><xsl:text>false</xsl:text></xsl:otherwise>
  </xsl:choose>
</xsl:template>

</xsl:stylesheet>
