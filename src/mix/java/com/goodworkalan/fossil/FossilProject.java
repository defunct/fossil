package com.goodworkalan.strata.mix;

import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.cookbook.JavaProject;

/**
 * Builds the project definition for Fossil.
 *
 * @author Alan Gutierrez
 */
public class FossilProject implements ProjectModule {
    /**
     * Build the project definition for Fossil.
     *
     * @param builder
     *          The project builder.
     */
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces("com.github.bigeasy.fossil/fossil/0.1")
                .depends()
                    .production("com.github.bigeasy.strata/strata/0.+1")
                    .production("com.github.bigeasy.pack/pack/0.+1")
                    .development("org.testng/testng-jdk15/5.10")
                    .development("org.mockito/mockito-core/1.6")
                    .end()
                .end()
            .end();
    }
}
