package dev.bytemate.byteorm.intellij

import com.intellij.psi.tree.IElementType

object ByteormTokenTypes {
    val DECLARATION_NAME = IElementType("BYTEORM_DECLARATION_NAME", ByteormLanguage)
    val WHITE_SPACE = IElementType("BYTEORM_WHITE_SPACE", ByteormLanguage)
    val COMMENT = IElementType("BYTEORM_COMMENT", ByteormLanguage)
    val KEYWORD = IElementType("BYTEORM_KEYWORD", ByteormLanguage)
    val ATTRIBUTE = IElementType("BYTEORM_ATTRIBUTE", ByteormLanguage)
    val MODIFIER = IElementType("BYTEORM_MODIFIER", ByteormLanguage)
    val TYPE = IElementType("BYTEORM_TYPE", ByteormLanguage)
    val ENTITY_TYPE = IElementType("BYTEORM_ENTITY_TYPE", ByteormLanguage)
    val IDENTIFIER = IElementType("BYTEORM_IDENTIFIER", ByteormLanguage)
    val STRING = IElementType("BYTEORM_STRING", ByteormLanguage)
    val LITERAL = IElementType("BYTEORM_LITERAL", ByteormLanguage)
    val QMARK = IElementType("BYTEORM_QMARK", ByteormLanguage)
    val L_BRACE = IElementType("BYTEORM_L_BRACE", ByteormLanguage)
    val R_BRACE = IElementType("BYTEORM_R_BRACE", ByteormLanguage)
    val L_PAREN = IElementType("BYTEORM_L_PAREN", ByteormLanguage)
    val R_PAREN = IElementType("BYTEORM_R_PAREN", ByteormLanguage)
    val COMMA = IElementType("BYTEORM_COMMA", ByteormLanguage)
    val DOT = IElementType("BYTEORM_DOT", ByteormLanguage)
    val COLON = IElementType("BYTEORM_COLON", ByteormLanguage)
    val EQUALS = IElementType("BYTEORM_EQUALS", ByteormLanguage)
    val AT = IElementType("BYTEORM_AT", ByteormLanguage)
    val BAD_CHARACTER = IElementType("BYTEORM_BAD_CHARACTER", ByteormLanguage)
}
