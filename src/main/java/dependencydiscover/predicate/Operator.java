package dependencydiscover.predicate;

public enum Operator {
    EQUAL, LESS, GREATER, LESSEQUAL, GREATEREQUAL, UNDEFINED, NOTEQUAL;

    public boolean violate(int op1, int op2) {
        switch (this) {
            case EQUAL:
                return op1 != op2;
            case LESS:
                return op1 >= op2;
            case GREATER:
                return op1 <= op2;
            case LESSEQUAL:
                return op1 > op2;
            case GREATEREQUAL:
                return op1 < op2;
            case NOTEQUAL:
                return op1 == op2;
            default:
                throw new RuntimeException("not isValid operator");
        }
    }

    public int toInt() {
        return this.ordinal();
    }

    public Operator reverse() {
        switch (this) {
            case EQUAL:
                return NOTEQUAL;
            case LESS:
                return GREATEREQUAL;
            case GREATER:
                return LESSEQUAL;
            case LESSEQUAL:
                return GREATER;
            case GREATEREQUAL:
                return LESS;
            case NOTEQUAL:
                return EQUAL;
            default:
                throw new RuntimeException("not isValid operator");
        }
    }

    @Override
    public String toString() {
        switch (this) {
            case UNDEFINED:
                return "?";
            case EQUAL:
                return "=";
            case LESS:
                return "<";
            case GREATER:
                return ">";
            case LESSEQUAL:
                return "<=";
            case GREATEREQUAL:
                return ">=";
            case NOTEQUAL:
                return "!=";
            default:
                throw new RuntimeException("not isValid operator");
        }
    }

    public static Operator fromString(String s) {
        switch (s) {
            case "=":
                return EQUAL;
            case "<":
                return LESS;
            case ">":
                return GREATER;
            case "<=":
                return LESSEQUAL;
            case ">=":
                return GREATEREQUAL;
            case "!=":
                return NOTEQUAL;
            default:
                throw new RuntimeException("invalid input " + s);
        }
    }

    public boolean isLessOrGreater() {
        return this == LESS || this == GREATER || this == GREATEREQUAL || this == LESSEQUAL;
    }
}
