<xsl:transform  version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="text" omit-xml-declaration="yes" />

<xsl:template match="change">version <xsl:value-of select="@id" />
<xsl:value-of select="text()" />
<xsl:text>&#13;&#10;</xsl:text>
</xsl:template>

<xsl:template match="node()">
	<xsl:apply-templates />
</xsl:template>

</xsl:transform >