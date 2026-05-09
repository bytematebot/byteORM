package dev.bytemate.byteorm.intellij

import com.intellij.lang.ASTNode
import com.intellij.lang.ParserDefinition
import com.intellij.lang.PsiParser
import com.intellij.lexer.Lexer
import com.intellij.openapi.project.Project
import com.intellij.psi.FileViewProvider
import com.intellij.psi.PsiElement
import com.intellij.psi.PsiFile
import com.intellij.psi.TokenType
import com.intellij.psi.impl.source.tree.LeafPsiElement
import com.intellij.psi.tree.IFileElementType
import com.intellij.psi.tree.TokenSet

class ByteormParserDefinition : ParserDefinition {
    override fun createLexer(project: Project?): Lexer = ByteormLexer()

    override fun createParser(project: Project?): PsiParser = ByteormParser()

    override fun getFileNodeType(): IFileElementType = ByteormFileElementType

    override fun getWhitespaceTokens(): TokenSet = WHITESPACE_TOKENS

    override fun getCommentTokens(): TokenSet = COMMENT_TOKENS

    override fun getStringLiteralElements(): TokenSet = STRING_TOKENS

    override fun createElement(node: ASTNode): PsiElement {
        return LeafPsiElement(node.elementType, node.text)
    }

    override fun createFile(viewProvider: FileViewProvider): PsiFile = ByteormFile(viewProvider)

    private companion object {
        val WHITESPACE_TOKENS: TokenSet = TokenSet.create(TokenType.WHITE_SPACE, ByteormTokenTypes.WHITE_SPACE)
        val COMMENT_TOKENS: TokenSet = TokenSet.create(ByteormTokenTypes.COMMENT)
        val STRING_TOKENS: TokenSet = TokenSet.create(ByteormTokenTypes.STRING)
    }
}
