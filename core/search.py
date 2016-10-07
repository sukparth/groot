from six import with_metaclass
from fnmatch import fnmatch
import re
from util import ReadOnlyClass


def regex_search(text, pattern=None):
    pattern = pattern or ".*"
    return re.search(pattern, text)


def glob_search(text, pattern=None):
    pattern = pattern or "*"
    return fnmatch(text, pattern)


class SearchKeys(with_metaclass(ReadOnlyClass, object)):
    """
    This is the base class for the key dictionary on which searches that be made
    Attributes are an empty dictionary of search keys.
    This class cannot be instantiated.
    """
    SEARCH_KEY_DICT = dict()

    def __new__(cls):
        raise RuntimeError('Initialize object for class {0} not permitted.'
                           .format(cls.__name__))

    @classmethod
    def get_value(cls, key):
        """ Returns value for a key """
        return cls.SEARCH_KEY_DICT[key]


class SearchOperators(with_metaclass(ReadOnlyClass, object)):
    """
    This class represents
    List of operation keys and their mappings to operations.
    This class cannot be instantiated.
    """
    OPER_DICT = dict()

    def __new__(cls):
        raise RuntimeError('Initialize object for class {0} not permitted.'
                           .format(cls.__name__))

    @classmethod
    def get_value(cls, key):
        return cls.OPER_DICT[key]


class SearchLogicalOperators(with_metaclass(ReadOnlyClass, object)):
    """
    List of logical operators and their precedence
    Class Attributes:
    LOGIC_DICT :  A dictionary with key as the logical operation
                  and value as precedence.
    """
    LOGIC_DICT = {"and": 1,
                  "or": 2}

    def __new__(cls):
        raise RuntimeError('Initialize object for class {0} not permitted.'
                           .format(cls.__name__))

    @classmethod
    def get_value(cls, key):
        return cls.LOGIC_DICT[key]

    @classmethod
    def __contains__(cls, key):
        return key in cls.LOGIC_DICT


class SearchExpression(object):
    """
    Creates a search expression for webhdfs data searches.

    Attributes:
        key(str)          : key field associated to the search expression.
        oper(str)         : The operation associated to the search expression.
                            The operations for string expressions can be
                            either 'regex' or 'glob'
                            The operations for numeric operators related
                            searches are '>', '<', '>=', '<=', '='
        val          :      The right side operand for the search expression.

    Example:
        # Path whose extension is either .dat, .csv or .txt
        path_search_exp = SearchExpression(PATH_KEY, "regex",
                                           "(.dat|.csv|.txt)$")

        # Size greater than 1GB
        path_search_exp = SearchExpression(SIZE_KEY,
                                           operator.gt,
                                           1073741824)
    """

    def __init__(self, key, oper, val, search_keys, search_operators):
        # print key, SearchKeys.SEARCH_KEY_DICT
        if key not in search_keys.SEARCH_KEY_DICT:
            raise ValueError("Invalid key provided")

        if oper not in search_operators.OPER_DICT:
            raise ValueError("Invalid operation")

        if not issubclass(search_keys, SearchKeys):
            raise ValueError("Search Key not of type or subclass"
                             " class SearchKeys or its derivative")

        if not issubclass(search_operators, SearchOperators):
            raise ValueError("Search Operation not of type or subclass"
                             " class SearchOperators or its derivative")

        self.key = search_keys.SEARCH_KEY_DICT[key]
        self.oper = search_operators.OPER_DICT[oper]
        self.val = val

    def match(self, dict_object):
        """
        Helper method
        Returns true if input search expression find a match in path_status
        else return False
        """
        if self.oper(dict_object[self.key], self.val):
            return True
        else:
            return False

    def __str__(self):
        return ",".join((str(self.key),
                         str(self.oper),
                         str(self.val)))


class SearchExpressionList(object):
    """ Contains a list of tokens in postfix order."""

    def __init__(self, search_logical_operators, search_expression=None):
        self.expr_list = []

        if not issubclass(search_logical_operators, SearchLogicalOperators):
            raise ValueError("search_logical_operators not of type or subclass"
                             "of class SearchLogicalOperations or its derivative")

        self.search_logical_operators = search_logical_operators
        if search_expression:
            self.add(search_expression)

    def add(self, token):
        """ Adds the token to the list if valid """
        if not isinstance(token, SearchExpression) and \
           token not in self.search_logical_operators.LOGIC_DICT:
                raise ValueError(token)

        self.expr_list.append(token)

    def add_expression(self, expression):
        """ Parser the expression and add it.
        Arguments:
            expression(list) : An expression list in infix format
                               The list can contain
                               1) Curved braces
                               2) Search Expression of type SearchExpression or its derivative
                               3) Logical Operator with string value "and" or "or"
        """
        pass

    def match(self, dict_object):
        """ Method to match the Search to evaluate postfix expression
            Algorithm:
                Initialize a stack
                Until end of expression list
                do
                  if token in list is a logical operator
                     pop 2 expressions and evaluate truth
                         based on the logical operator
                  else if token in list is an expression
                     evaluate expression and push bool result to stack.
                done
                If the stack has only one result of type bool
                    return that
                else
                    Raise an error
        """

        se_stack = []  # to store intermediate expression results

        for i in range(0, len(self.expr_list)):
            if self.expr_list[i] in self.search_logical_operators.LOGIC_DICT:
                try:
                    search_exp1_truth = se_stack.pop()
                    search_exp2_truth = se_stack.pop()
                    if self.expr_list[i] == 'and':
                        se_stack.append(search_exp1_truth and
                                        search_exp2_truth)
                    else:
                        se_stack.append(search_exp1_truth or search_exp2_truth)
                except:
                    raise ValueError("Search Expression incorrect")

            else:  # If token is an expression
                se_stack.append(self.expr_list[i].match(dict_object))

        if len(se_stack) != 1 and not isinstance(se_stack[0], bool):
            raise ValueError("Search Expression incorrect")
        else:
            # print se_stack[0]
            return se_stack[0]

            # print(path_status[search_exp.key], search_exp.val)

    def __str__(self):
        return ",".join(str(x) for x in self.expr_list)
