# Generated from grammar/DynamoDbGrammar.g4 by ANTLR 4.7
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .DynamoDbGrammarParser import DynamoDbGrammarParser
else:
    from DynamoDbGrammarParser import DynamoDbGrammarParser

# This class defines a complete listener for a parse tree produced by DynamoDbGrammarParser.
class DynamoDbGrammarListener(ParseTreeListener):

    # Enter a parse tree produced by DynamoDbGrammarParser#projection_.
    def enterProjection_(self, ctx:DynamoDbGrammarParser.Projection_Context):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#projection_.
    def exitProjection_(self, ctx:DynamoDbGrammarParser.Projection_Context):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#projection.
    def enterProjection(self, ctx:DynamoDbGrammarParser.ProjectionContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#projection.
    def exitProjection(self, ctx:DynamoDbGrammarParser.ProjectionContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#condition_.
    def enterCondition_(self, ctx:DynamoDbGrammarParser.Condition_Context):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#condition_.
    def exitCondition_(self, ctx:DynamoDbGrammarParser.Condition_Context):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#Or.
    def enterOr(self, ctx:DynamoDbGrammarParser.OrContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#Or.
    def exitOr(self, ctx:DynamoDbGrammarParser.OrContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#Negation.
    def enterNegation(self, ctx:DynamoDbGrammarParser.NegationContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#Negation.
    def exitNegation(self, ctx:DynamoDbGrammarParser.NegationContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#In.
    def enterIn(self, ctx:DynamoDbGrammarParser.InContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#In.
    def exitIn(self, ctx:DynamoDbGrammarParser.InContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#And.
    def enterAnd(self, ctx:DynamoDbGrammarParser.AndContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#And.
    def exitAnd(self, ctx:DynamoDbGrammarParser.AndContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#Between.
    def enterBetween(self, ctx:DynamoDbGrammarParser.BetweenContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#Between.
    def exitBetween(self, ctx:DynamoDbGrammarParser.BetweenContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#FunctionCondition.
    def enterFunctionCondition(self, ctx:DynamoDbGrammarParser.FunctionConditionContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#FunctionCondition.
    def exitFunctionCondition(self, ctx:DynamoDbGrammarParser.FunctionConditionContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#Comparator.
    def enterComparator(self, ctx:DynamoDbGrammarParser.ComparatorContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#Comparator.
    def exitComparator(self, ctx:DynamoDbGrammarParser.ComparatorContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#ConditionGrouping.
    def enterConditionGrouping(self, ctx:DynamoDbGrammarParser.ConditionGroupingContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#ConditionGrouping.
    def exitConditionGrouping(self, ctx:DynamoDbGrammarParser.ConditionGroupingContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#comparator_symbol.
    def enterComparator_symbol(self, ctx:DynamoDbGrammarParser.Comparator_symbolContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#comparator_symbol.
    def exitComparator_symbol(self, ctx:DynamoDbGrammarParser.Comparator_symbolContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#update_.
    def enterUpdate_(self, ctx:DynamoDbGrammarParser.Update_Context):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#update_.
    def exitUpdate_(self, ctx:DynamoDbGrammarParser.Update_Context):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#update.
    def enterUpdate(self, ctx:DynamoDbGrammarParser.UpdateContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#update.
    def exitUpdate(self, ctx:DynamoDbGrammarParser.UpdateContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#set_section.
    def enterSet_section(self, ctx:DynamoDbGrammarParser.Set_sectionContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#set_section.
    def exitSet_section(self, ctx:DynamoDbGrammarParser.Set_sectionContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#set_action.
    def enterSet_action(self, ctx:DynamoDbGrammarParser.Set_actionContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#set_action.
    def exitSet_action(self, ctx:DynamoDbGrammarParser.Set_actionContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#add_section.
    def enterAdd_section(self, ctx:DynamoDbGrammarParser.Add_sectionContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#add_section.
    def exitAdd_section(self, ctx:DynamoDbGrammarParser.Add_sectionContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#add_action.
    def enterAdd_action(self, ctx:DynamoDbGrammarParser.Add_actionContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#add_action.
    def exitAdd_action(self, ctx:DynamoDbGrammarParser.Add_actionContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#delete_section.
    def enterDelete_section(self, ctx:DynamoDbGrammarParser.Delete_sectionContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#delete_section.
    def exitDelete_section(self, ctx:DynamoDbGrammarParser.Delete_sectionContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#delete_action.
    def enterDelete_action(self, ctx:DynamoDbGrammarParser.Delete_actionContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#delete_action.
    def exitDelete_action(self, ctx:DynamoDbGrammarParser.Delete_actionContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#remove_section.
    def enterRemove_section(self, ctx:DynamoDbGrammarParser.Remove_sectionContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#remove_section.
    def exitRemove_section(self, ctx:DynamoDbGrammarParser.Remove_sectionContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#remove_action.
    def enterRemove_action(self, ctx:DynamoDbGrammarParser.Remove_actionContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#remove_action.
    def exitRemove_action(self, ctx:DynamoDbGrammarParser.Remove_actionContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#OperandValue.
    def enterOperandValue(self, ctx:DynamoDbGrammarParser.OperandValueContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#OperandValue.
    def exitOperandValue(self, ctx:DynamoDbGrammarParser.OperandValueContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#ArithmeticValue.
    def enterArithmeticValue(self, ctx:DynamoDbGrammarParser.ArithmeticValueContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#ArithmeticValue.
    def exitArithmeticValue(self, ctx:DynamoDbGrammarParser.ArithmeticValueContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#PlusMinus.
    def enterPlusMinus(self, ctx:DynamoDbGrammarParser.PlusMinusContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#PlusMinus.
    def exitPlusMinus(self, ctx:DynamoDbGrammarParser.PlusMinusContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#ArithmeticParens.
    def enterArithmeticParens(self, ctx:DynamoDbGrammarParser.ArithmeticParensContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#ArithmeticParens.
    def exitArithmeticParens(self, ctx:DynamoDbGrammarParser.ArithmeticParensContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#PathOperand.
    def enterPathOperand(self, ctx:DynamoDbGrammarParser.PathOperandContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#PathOperand.
    def exitPathOperand(self, ctx:DynamoDbGrammarParser.PathOperandContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#LiteralOperand.
    def enterLiteralOperand(self, ctx:DynamoDbGrammarParser.LiteralOperandContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#LiteralOperand.
    def exitLiteralOperand(self, ctx:DynamoDbGrammarParser.LiteralOperandContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#FunctionOperand.
    def enterFunctionOperand(self, ctx:DynamoDbGrammarParser.FunctionOperandContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#FunctionOperand.
    def exitFunctionOperand(self, ctx:DynamoDbGrammarParser.FunctionOperandContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#ParenOperand.
    def enterParenOperand(self, ctx:DynamoDbGrammarParser.ParenOperandContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#ParenOperand.
    def exitParenOperand(self, ctx:DynamoDbGrammarParser.ParenOperandContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#FunctionCall.
    def enterFunctionCall(self, ctx:DynamoDbGrammarParser.FunctionCallContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#FunctionCall.
    def exitFunctionCall(self, ctx:DynamoDbGrammarParser.FunctionCallContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#path.
    def enterPath(self, ctx:DynamoDbGrammarParser.PathContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#path.
    def exitPath(self, ctx:DynamoDbGrammarParser.PathContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#id_.
    def enterId_(self, ctx:DynamoDbGrammarParser.Id_Context):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#id_.
    def exitId_(self, ctx:DynamoDbGrammarParser.Id_Context):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#MapAccess.
    def enterMapAccess(self, ctx:DynamoDbGrammarParser.MapAccessContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#MapAccess.
    def exitMapAccess(self, ctx:DynamoDbGrammarParser.MapAccessContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#ListAccess.
    def enterListAccess(self, ctx:DynamoDbGrammarParser.ListAccessContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#ListAccess.
    def exitListAccess(self, ctx:DynamoDbGrammarParser.ListAccessContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#LiteralSub.
    def enterLiteralSub(self, ctx:DynamoDbGrammarParser.LiteralSubContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#LiteralSub.
    def exitLiteralSub(self, ctx:DynamoDbGrammarParser.LiteralSubContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#expression_attr_names_sub.
    def enterExpression_attr_names_sub(self, ctx:DynamoDbGrammarParser.Expression_attr_names_subContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#expression_attr_names_sub.
    def exitExpression_attr_names_sub(self, ctx:DynamoDbGrammarParser.Expression_attr_names_subContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#expression_attr_values_sub.
    def enterExpression_attr_values_sub(self, ctx:DynamoDbGrammarParser.Expression_attr_values_subContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#expression_attr_values_sub.
    def exitExpression_attr_values_sub(self, ctx:DynamoDbGrammarParser.Expression_attr_values_subContext):
        pass


    # Enter a parse tree produced by DynamoDbGrammarParser#unknown.
    def enterUnknown(self, ctx:DynamoDbGrammarParser.UnknownContext):
        pass

    # Exit a parse tree produced by DynamoDbGrammarParser#unknown.
    def exitUnknown(self, ctx:DynamoDbGrammarParser.UnknownContext):
        pass


