package dependencyDiscover.Validator;

import dependencyDiscover.Data.DataFrame;
import dependencyDiscover.Dependency.AbstractDependency;
import dependencyDiscover.Validator.Result.ApproximateDependencyValidationResult;

public interface ApproximateDependencyValidator<E extends AbstractDependency> extends AbstractValidator {

    ApproximateDependencyValidationResult validate(DataFrame data, E dependency);
}
