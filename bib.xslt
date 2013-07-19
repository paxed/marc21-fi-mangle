<?xml version="1.0" encoding="UTF-8"?>

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
      <xsl:element name="field">
        <xsl:attribute name="tag"><xsl:value-of select="./@tag"/></xsl:attribute>
        <xsl:attribute name="repeatable"><xsl:call-template name="mangle_repeatable_YN" /></xsl:attribute>
        <xsl:element name="name"><xsl:value-of select="./name"/></xsl:element>
        <xsl:element name="description"><xsl:value-of select="./description"/></xsl:element>
        <xsl:call-template name="parse_indicators" />
        <xsl:call-template name="parse_subfields" />
      </xsl:element>
    </xsl:for-each>
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
   <xsl:element name="description"><xsl:value-of select="description"/></xsl:element>
   </xsl:element>
  </xsl:for-each>
</xsl:template>


<xsl:template name="parse_subfields">
 <xsl:for-each select="./subfields/subfield">
   <xsl:element name="subfield">
     <xsl:attribute name="code"><xsl:value-of select="./@code"/></xsl:attribute>
     <xsl:attribute name="repeatable"><xsl:call-template name="mangle_repeatable_YN" /></xsl:attribute>
     <xsl:element name="description">
       <xsl:value-of select="name"/><xsl:if test="description">: </xsl:if>
       <xsl:value-of select="description"/>
     </xsl:element>
   </xsl:element>
 </xsl:for-each>
</xsl:template>


<xsl:template name="mangle_repeatable_YN">
  <xsl:if test="./@repeatable = 'Y'"><xsl:text>true</xsl:text></xsl:if>
  <xsl:if test="./@repeatable = 'N'"><xsl:text>false</xsl:text></xsl:if>
</xsl:template>

</xsl:stylesheet>