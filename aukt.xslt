<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:import href="common.xslt"/>

<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>

<xsl:template match="/">
  <xsl:element name="fields">
  <xsl:copy>
    <xsl:apply-templates select="document('data/aukt-000.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-00X.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-01X-09X.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-1XX.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-2XX-3XX.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-4XX.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-5XX.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-64X.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-663-666.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-667-68X.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-7XX.xml')/fields"/>
    <xsl:apply-templates select="document('data/aukt-8XX.xml')/fields"/>
  </xsl:copy>
  </xsl:element>
</xsl:template>

</xsl:stylesheet>
