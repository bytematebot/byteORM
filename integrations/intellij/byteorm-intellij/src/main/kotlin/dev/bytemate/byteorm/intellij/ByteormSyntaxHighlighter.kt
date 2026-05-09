package dev.bytemate.byteorm.intellij

import com.intellij.lexer.Lexer
import com.intellij.openapi.editor.colors.TextAttributesKey
import com.intellij.openapi.editor.markup.EffectType
import com.intellij.openapi.editor.markup.TextAttributes
import com.intellij.openapi.fileTypes.SyntaxHighlighterBase
import com.intellij.psi.TokenType
import com.intellij.psi.tree.IElementType
import java.awt.Color
import java.awt.Font

class ByteormSyntaxHighlighter(
    private val symbols: ByteormSymbols?,
) : SyntaxHighlighterBase() {
    override fun getHighlightingLexer(): Lexer = ByteormLexer(symbols)

    override fun getTokenHighlights(tokenType: IElementType?): Array<TextAttributesKey> {
        return when (tokenType) {
            ByteormTokenTypes.KEYWORD -> KEYWORD_KEYS
            ByteormTokenTypes.ATTRIBUTE -> ATTRIBUTE_KEYS
            ByteormTokenTypes.MODIFIER -> MODIFIER_KEYS
            ByteormTokenTypes.TYPE -> TYPE_KEYS
            ByteormTokenTypes.DECLARATION_NAME -> DECLARATION_NAME_KEYS
            ByteormTokenTypes.IDENTIFIER -> IDENTIFIER_KEYS
            ByteormTokenTypes.STRING -> STRING_KEYS
            ByteormTokenTypes.LITERAL -> LITERAL_KEYS
            ByteormTokenTypes.QMARK -> QMARK_KEYS
            ByteormTokenTypes.COMMENT -> COMMENT_KEYS
            ByteormTokenTypes.L_BRACE, ByteormTokenTypes.R_BRACE -> BRACE_KEYS
            ByteormTokenTypes.L_PAREN, ByteormTokenTypes.R_PAREN -> PAREN_KEYS
            ByteormTokenTypes.COMMA, ByteormTokenTypes.DOT, ByteormTokenTypes.COLON, ByteormTokenTypes.EQUALS, ByteormTokenTypes.AT -> PUNCTUATION_KEYS
            TokenType.BAD_CHARACTER -> BAD_CHARACTER_KEYS
            else -> EMPTY_KEYS
        }
    }

    private companion object {
        val KEYWORD = key("BYTEORM_KEYWORD", 0x3B, 0x82, 0xF6)
        val ATTRIBUTE = key("BYTEORM_ATTRIBUTE", 0x22, 0xC5, 0x5E)
        val MODIFIER = key("BYTEORM_MODIFIER", 0xF59E0B)
        val TYPE = key("BYTEORM_TYPE", 0x22, 0xD3, 0xEE)
        val DECLARATION_NAME = key("BYTEORM_DECLARATION_NAME", 0x86, 0xEF, 0xAC)
        val IDENTIFIER = key("BYTEORM_IDENTIFIER", 0xF8, 0xFA, 0xFC)
        val STRING = key("BYTEORM_STRING", 0xF9, 0xA8, 0xD4)
        val LITERAL = key("BYTEORM_LITERAL", 0xD9, 0x46, 0xEF)
        val COMMENT = key("BYTEORM_COMMENT", 0x94, 0xA3, 0xB8)
        val BRACE = key("BYTEORM_BRACE", 0xEAB308)
        val PAREN = key("BYTEORM_PAREN", 0xC0, 0x84, 0xFC)
        val PUNCTUATION = key("BYTEORM_PUNCTUATION", 0x64, 0x74, 0x8B)
        val QMARK = key("BYTEORM_QMARK", 0xD8, 0x3B, 0xC9)
        val BAD_CHARACTER = TextAttributesKey.createTextAttributesKey(
            "BYTEORM_BAD_CHARACTER",
            TextAttributes(Color.RED, null, null, EffectType.WAVE_UNDERSCORE, Font.PLAIN),
        )

        val KEYWORD_KEYS = arrayOf(KEYWORD)
        val ATTRIBUTE_KEYS = arrayOf(ATTRIBUTE)
        val MODIFIER_KEYS = arrayOf(MODIFIER)
        val TYPE_KEYS = arrayOf(TYPE)
        val DECLARATION_NAME_KEYS = arrayOf(DECLARATION_NAME)
        val IDENTIFIER_KEYS = arrayOf(IDENTIFIER)
        val STRING_KEYS = arrayOf(STRING)
        val LITERAL_KEYS = arrayOf(LITERAL)
        val QMARK_KEYS = arrayOf(QMARK)
        val COMMENT_KEYS = arrayOf(COMMENT)
        val BRACE_KEYS = arrayOf(BRACE)
        val PAREN_KEYS = arrayOf(PAREN)
        val PUNCTUATION_KEYS = arrayOf(PUNCTUATION)
        val BAD_CHARACTER_KEYS = arrayOf(BAD_CHARACTER)
        val EMPTY_KEYS = emptyArray<TextAttributesKey>()

        fun key(name: String, rgb: Int): TextAttributesKey {
            val red = (rgb shr 16) and 0xFF
            val green = (rgb shr 8) and 0xFF
            val blue = rgb and 0xFF
            return key(name, red, green, blue)
        }

        fun key(name: String, red: Int, green: Int, blue: Int): TextAttributesKey {
            return TextAttributesKey.createTextAttributesKey(
                name,
                TextAttributes(Color(red, green, blue), null, null, null, Font.PLAIN),
            )
        }
    }
}
